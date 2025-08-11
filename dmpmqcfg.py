#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
 Script Name   : dmpmqcfg.py
 Description   : Dump IBM MQ queue manager configuration in MQSC (default) or
                 JSON using REST API or pymqi.
 Author        : Rob Lee
===============================================================================

Modification History:
 Date        | Author   | Description
-------------|----------|------------------------------------------------------
2025-08-11   | Rob Lee  | Initial commit - author header + history section
2025-08-11   | Rob Lee  | Added --format (mqsc|json), MQ_FORMAT env var, and
                        | MQSC formatter (default output)
===============================================================================

This script provides a robust, enterprise-grade alternative to IBM MQ's `dmpmqcfg`
command, retrieving queue manager object definitions in either **MQSC** (default)
or **JSON**. It supports two modes: REST API (default) or pymqi (using standard
MQ API via local or remote connection). Configuration can be provided via
command-line arguments or environment variables.

Prerequisites:
    - Python 3.8+
    - Required packages: `requests`, `python-dotenv`
    - For pymqi mode: `pymqi` (IBM MQ client libraries must be installed and configured)
    - Optional package: `orjson` (for faster JSON processing)
    - IBM MQ queue manager with REST API enabled (for REST mode) or accessible via MQ API (for pymqi mode)
    - Valid user credentials for authentication

Usage:
    Run the script with command-line arguments or environment variables:
        python3 dmpmqcfg.py --mode rest|pymqi --format mqsc|json -m QMGR_NAME -u USER -c PASS -s SERVER -p PORT

    For pymqi mode (local): 
        python3 dmpmqcfg.py --mode pymqi -m QM1
    For pymqi mode (remote): 
        python3 dmpmqcfg.py --mode pymqi --conn-type remote -m QM1 -u user -c pass --host localhost --conn-port 1414 --channel SYSTEM.DEF.SVRCONN

    Environment variables (in .env file or system):
        MQ_MODE, MQ_QMGR, MQ_USER, MQ_PASSWORD, MQ_SERVER, MQ_PORT,
        MQ_CONN_TYPE, MQ_HOST, MQ_CONN_PORT, MQ_CHANNEL, MQ_FORMAT

Output:
    - MQSC statements (default), printed to stdout
    - Or JSON when --format json is specified
    Errors are logged to `dmpmqcfg.log` and displayed in JSON format.

Notes:
    - SSL verification is disabled for REST mode (use with caution).
    - The MQSC output is suitable for review and as a starting point; validate before replaying.
    - For z/OS queue managers, adjust the OBJECT_TYPES list as needed.
    - In pymqi mode, default attributes are returned (not all); extend with custom attribute lists if needed.
    - For AUTHREC in pymqi mode, inquires for both principals and groups.
    - For SUB in pymqi mode, filters to admin subscriptions post-inquiry.
"""

import os
import sys
import logging
from typing import Dict, List, Optional, Iterable
from pathlib import Path
import argparse
from tempfile import TemporaryDirectory
import re
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout
from dotenv import load_dotenv

try:
    import orjson as json
except ImportError:
    import json

# Configure logging with structured format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('dmpmqcfg.log')
    ]
)
logger = logging.getLogger(__name__)

# -------------------------
# MQSC formatting utilities
# -------------------------

# Best-effort mapping of "primary name" attribute per object type.
PRIMARY_NAME_KEY = {
    'QMGR': None,
    'QUEUE': 'qname',
    'CHANNEL': 'channelname',
    'CHANNEL_CLIENT': 'channelname',
    'PROCESS': 'processname',
    'NAMELIST': 'namelistname',
    'LISTENER': 'listenername',
    'SERVICE': 'servicename',
    'TOPIC': 'topicname',
    'AUTHINFO': 'authinfoname',
    'SUB': 'subname',
    'CHLAUTH': 'channelname',
    'POLICY': 'policyname',
}

# If REST returns generic "name" instead of specific keys, we’ll fall back to this
FALLBACK_NAME_KEYS = ('name', 'qname', 'channelname', 'processname', 'namelistname',
                      'listenername', 'servicename', 'topicname', 'authinfoname',
                      'subname', 'policyname')

TOKEN_RE = re.compile(r'^[A-Za-z0-9._%/+:-]+$')

def _fmt_value(v) -> str:
    """Format a value for MQSC. Quote strings that have spaces or non-token chars."""
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, bool):
        # MQSC typically expects numeric or specific constants; leave as 1/0 for booleans.
        return '1' if v else '0'
    if v is None:
        return '""'
    s = str(v)
    if TOKEN_RE.match(s) and ' ' not in s:
        return s
    # escape double quotes inside the string
    s = s.replace('"', r'\"')
    return f'"{s}"'

def _attr_key_to_mqsc(k: str) -> str:
    """Convert attribute key to MQSC-style uppercase token."""
    return k.replace('_', '').upper()

def _detect_name(obj_type: str, params: Dict) -> Optional[str]:
    pk = PRIMARY_NAME_KEY.get(obj_type)
    if pk and pk in params:
        return params.get(pk)
    # fallback detection
    for k in FALLBACK_NAME_KEYS:
        if k in params:
            return params[k]
    return None

def objects_to_mqsc(obj_type: str, objects: Iterable[Dict]) -> List[str]:
    """Turn a list of object parameter dicts into MQSC DEFINE/ALTER statements."""
    lines: List[str] = []
    for p in objects:
        params = dict(p) if isinstance(p, dict) else {}
        name = _detect_name(obj_type, params)

        # Remove name field from attributes to avoid duplication in key/value list
        for k in list(params.keys()):
            if k in FALLBACK_NAME_KEYS or k == 'objecttype':
                params.pop(k, None)

        if obj_type == 'QMGR':
            # Represent qmgr attributes as ALTER QMGR ...
            kv = ' '.join(f"{_attr_key_to_mqsc(k)}({_fmt_value(v)})" for k, v in sorted(params.items()))
            if kv:
                lines.append(f"ALTER QMGR {kv}")
            continue

        qualifier = 'CHANNEL' if obj_type == 'CHANNEL_CLIENT' else obj_type
        base = f"DEFINE {qualifier}"
        if name:
            base += f"({name})"

        # For CHLAUTH, use SET CHLAUTH rather than DEFINE (dmpmqcfg uses SET)
        if obj_type == 'CHLAUTH':
            base = "SET CHLAUTH"
            if name:
                base += f"('{name}')"

        # Build attributes
        kv_pairs = []
        for k, v in sorted(params.items()):
            # Skip some noisy fields that often come in as computed-only
            if k in {'definitiontype', 'objecttype', 'distlists'}:
                continue
            kv_pairs.append(f"{_attr_key_to_mqsc(k)}({_fmt_value(v)})")

        if kv_pairs:
            lines.append(f"{base} {' '.join(kv_pairs)}")
        else:
            lines.append(base)

    return lines

# -------------------------

class MQConfigDumper:
    """Utility to dump IBM MQ queue manager configuration via REST API or pymqi.

    Produces MQSC (default) or JSON depending on --format.
    """

    DEFAULT_CONFIG = {
        'qmgr': 'QM1',
        'user': 'mqguest',
        'password': 'passw0rd',
        'server': 'localhost',
        'port': 9443,
        'timeout': 30,
        'mode': 'rest',
        'conn_type': 'local',
        'host': 'localhost',
        'conn_port': 1414,
        'channel': 'SYSTEM.DEF.SVRCONN',
        'out_format': 'mqsc',   # mqsc | json
    }

    OBJECT_TYPES = [
        'QMGR', 'QUEUE', 'CHANNEL', 'CHANNEL_CLIENT', 'PROCESS', 'NAMELIST',
        'LISTENER', 'SERVICE', 'TOPIC', 'AUTHINFO', 'AUTHREC', 'SUB', 'CHLAUTH',
        'POLICY'
    ]

    def __init__(self, config: Dict[str, str]) -> None:
        self.config = config
        self.mode = self.config['mode']
        self.out_format = (self.config.get('out_format') or 'mqsc').lower()

        if self.mode == 'rest':
            self.base_url = (
                f"https://{self.config['server']}:{self.config['port']}"
                f"/ibmmq/rest/v2/admin/action/qmgr/{self.config['qmgr']}/mqsc"
            )
            self.session = requests.Session()
            self.session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'ibm-mq-rest-csrf-token': 'random'
            })
            self.session.auth = (self.config['user'], self.config['password'])
            self.session.verify = False  # Disable SSL verification (use with caution)
            self.qmgr = None
        elif self.mode == 'pymqi':
            try:
                import pymqi
                from pymqi import CMQC, CMQCFC, MQMIError
                self.pymqi = pymqi
                self.CMQC = CMQC
                self.CMQCFC = CMQCFC
                self.MQMIError = MQMIError
            except ImportError:
                raise ImportError("pymqi module is required for 'pymqi' mode. Please install it.")
            self.attr_map = {v: k for k, v in vars(self.CMQC).items() if k.startswith('MQ') and isinstance(v, int)}
            if self.config['conn_type'] == 'local':
                self.qmgr = self.pymqi.QueueManager(self.config['qmgr'])
            else:
                conn_info = f"{self.config['host']}({self.config['conn_port']})"
                self.qmgr = self.pymqi.connect(
                    self.config['qmgr'],
                    self.config['channel'],
                    conn_info,
                    self.config['user'],
                    self.config['password']
                )
            self.session = None
            self.base_url = None
        else:
            raise ValueError("Invalid mode: must be 'rest' or 'pymqi'")

    def validate_config(self) -> None:
        if not self.config['qmgr']:
            raise ValueError("Queue manager name cannot be empty")
        if self.out_format not in ('mqsc', 'json'):
            raise ValueError("--format must be 'mqsc' or 'json'")
        if self.mode == 'rest':
            if not (1 <= self.config['port'] <= 65535):
                raise ValueError("Port must be between 1 and 65535")
            if not self.config['user'] or not self.config['password']:
                raise ValueError("Username and password are required for REST mode")
        elif self.mode == 'pymqi':
            if self.config['conn_type'] == 'remote':
                if not self.config['host']:
                    raise ValueError("Host is required for remote pymqi connection")
                if not (1 <= self.config['conn_port'] <= 65535):
                    raise ValueError("Connection port must be between 1 and 65535")
                if not self.config['channel']:
                    raise ValueError("Channel is required for remote pymqi connection")
                if not self.config['user'] or not self.config['password']:
                    raise ValueError("Username and password are required for remote pymqi connection")

    def build_json_command(self, obj_type: str) -> Dict:
        name_line = {"name": "*"} if obj_type not in ['QMGR', 'AUTHREC'] else {}
        resp_line = {"responseParameters": ["ALL"]} if obj_type != 'POLICY' else {}

        if obj_type == 'CHANNEL_CLIENT':
            obj_type = 'CHANNEL'
            filt_line = {"parameters": {"chltype": "clntconn"}}
        elif obj_type == 'SUB':
            filt_line = {"parameters": {"subtype": "admin"}}
        else:
            filt_line = {}

        return {
            "type": "runCommandJSON",
            "command": "DISPLAY",
            "qualifier": obj_type,
            **name_line,
            **resp_line,
            **filt_line
        }

    def process_rest_response(self, obj_type: str, response: requests.Response) -> List[Dict]:
        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Failed to parse JSON response for {obj_type}: {e}")
            return [{
                "queueManager": self.config['qmgr'],
                "objectType": obj_type,
                "error": "Invalid JSON response"
            }]

        result = {
            "queueManager": self.config['qmgr'],
            "objectType": obj_type,
            "objects": data.get('commandResponse', [])
        }

        if data.get('overallCompletionCode') == 0:
            for obj in result['objects']:
                obj.pop('completionCode', None)
                obj.pop('reasonCode', None)
                obj['parameters'] = obj.get('parameters', {})
            result['objects'] = [obj['parameters'] for obj in result['objects']]
        else:
            result.update({
                'completionCode': data.get('overallCompletionCode'),
                'reasonCode': data.get('overallReasonCode'),
                'error': data.get('error', 'Unknown error')
            })
            result.pop('objects', None)

        return [result]

    def attr_to_str(self, key: int) -> str:
        name = self.attr_map.get(key, str(key))
        if name.startswith(('MQCA_', 'MQIA_', 'MQGA_', 'MQBACF_')):
            prefix_len = name.find('_') + 1
            rest = name[prefix_len:]
            parts = [part.lower() for part in rest.split('_')]
            return ''.join(parts)
        return name.lower()

    def process_pymqi_response(self, obj_type: str, response: List[Dict]) -> List[Dict]:
        objects = []
        for item in response:
            params = {
                self.attr_to_str(k): v.decode('utf-8').strip() if isinstance(v, bytes) else v
                for k, v in item.items()
            }
            objects.append(params)

        if obj_type == 'SUB':
            objects = [o for o in objects if o.get('subtype') == 1]

        return [{
            "queueManager": self.config['qmgr'],
            "objectType": obj_type,
            "objects": objects
        }]

    def get_pymqi_config(self, obj_type: str) -> tuple:
        configs = {
            'QMGR': (self.CMQC.MQCMD_INQUIRE_Q_MGR, {}),
            'QUEUE': (self.CMQC.MQCMD_INQUIRE_Q, {self.CMQC.MQCA_Q_NAME: b'*'}),
            'CHANNEL': (self.CMQC.MQCMD_INQUIRE_CHANNEL, {self.CMQC.MQCA_CHANNEL_NAME: b'*'}),
            'CHANNEL_CLIENT': (self.CMQC.MQCMD_INQUIRE_CHANNEL, {
                self.CMQC.MQCA_CHANNEL_NAME: b'*',
                self.CMQC.MQIA_CHANNEL_TYPE: self.CMQC.MQCHT_CLNTCONN
            }),
            'PROCESS': (self.CMQC.MQCMD_INQUIRE_PROCESS, {self.CMQC.MQCA_PROCESS_NAME: b'*'}),
            'NAMELIST': (self.CMQC.MQCMD_INQUIRE_NAMELIST, {self.CMQC.MQCA_NAMELIST_NAME: b'*'}),
            'LISTENER': (self.CMQC.MQCMD_INQUIRE_LISTENER, {self.CMQC.MQCA_LISTENER_NAME: b'*'}),
            'SERVICE': (self.CMQC.MQCMD_INQUIRE_SERVICE, {self.CMQC.MQCA_SERVICE_NAME: b'*'}),
            'TOPIC': (self.CMQC.MQCMD_INQUIRE_TOPIC, {self.CMQC.MQCA_TOPIC_NAME: b'*'}),
            'AUTHINFO': (self.CMQC.MQCMD_INQUIRE_AUTH_INFO, {self.CMQC.MQCA_AUTH_INFO_NAME: b'*'}),
            'SUB': (self.CMQC.MQCMD_INQUIRE_SUB, {self.CMQC.MQCA_SUB_NAME: b'*'}),
            'CHLAUTH': (self.CMQC.MQCMD_INQUIRE_CHLAUTH_RECS, {self.CMQC.MQCA_CHANNEL_NAME: b'*'}),
            'POLICY': (self.CMQC.MQCMD_INQUIRE_PROT_POLICY, {self.CMQC.MQCA_POLICY_NAME: b'*'}),
            'AUTHREC': (self.CMQC.MQCMD_INQUIRE_AUTH_RECS, {
                self.CMQC.MQCACF_AUTH_PROFILE_NAME: b'*',
                self.CMQC.MQCACF_ENTITY_NAME: b'*',
                self.CMQC.MQIACF_OBJECT_TYPE: self.CMQC.MQOT_ALL}),
        }
        return configs.get(obj_type, (None, None))

    def _emit(self, result: Dict) -> None:
        """Emit either MQSC or JSON for a single result block {objectType, objects}."""
        if 'objects' not in result:
            # error case is already JSON; print as JSON regardless of format
            print(json.dumps(result, indent=2))
            return

        if self.out_format == 'json':
            print(json.dumps(result, indent=2))
            return

        # MQSC
        obj_type = result['objectType']
        lines = objects_to_mqsc(obj_type, result['objects'])
        for line in lines:
            print(line)

    def dump_config(self) -> None:
        if self.mode == 'rest':
            self._dump_rest()
        elif self.mode == 'pymqi':
            self._dump_pymqi()

    def _dump_rest(self) -> None:
        with TemporaryDirectory(prefix='dmpmqcfg_') as tmp_dir:
            logger.info(f"Using temporary directory: {tmp_dir}")

            for obj_type in self.OBJECT_TYPES:
                logger.info(f"Processing object type: {obj_type}")
                cmd = self.build_json_command(obj_type)

                cmd_file = Path(tmp_dir) / 'curl.in'
                with cmd_file.open('wb') as f:
                    payload = json.dumps(cmd) if hasattr(json, 'dumps') else json.dumps(cmd).encode()
                    f.write(payload)

                try:
                    response = self.session.post(
                        self.base_url,
                        data=cmd_file.read_bytes(),
                        timeout=self.config['timeout']
                    )
                    response.raise_for_status()

                    results = self.process_rest_response(obj_type, response)
                    for result in results:
                        logger.debug(f"Output for {obj_type}: {json.dumps(result)}")
                        self._emit(result)

                except (HTTPError, ConnectionError, Timeout) as e:
                    logger.error(f"Failed to process {obj_type}: {e}")
                    print(json.dumps({
                        "queueManager": self.config['qmgr'],
                        "objectType": obj_type,
                        "error": str(e)
                    }, indent=2))
                    sys.exit(1)
                except Exception as e:
                    logger.error(f"Unexpected error for {obj_type}: {e}", exc_info=True)
                    print(json.dumps({
                        "queueManager": self.config['qmgr'],
                        "objectType": obj_type,
                        "error": "Unexpected error occurred"
                    }, indent=2))
                    sys.exit(1)

    def _dump_pymqi(self) -> None:
        pcf = self.pymqi.PCFExecute(self.qmgr)

        for obj_type in self.OBJECT_TYPES:
            logger.info(f"Processing object type: {obj_type}")
            cmd, params = self.get_pymqi_config(obj_type)
            if cmd is None:
                logger.warning(f"Skipping unsupported object type in pymqi mode: {obj_type}")
                continue

            try:
                if obj_type == 'AUTHREC':
                    params_base = params.copy()
                    params_p = params_base.copy()
                    params_p[self.CMQC.MQIACF_ENTITY_TYPE] = self.CMQC.MQZAET_PRINCIPAL
                    response_p = pcf(cmd, params_p)

                    params_g = params_base.copy()
                    params_g[self.CMQC.MQIACF_ENTITY_TYPE] = self.CMQC.MQZAET_GROUP
                    response_g = pcf(cmd, params_g)

                    response = response_p + response_g
                else:
                    response = pcf(cmd, params)

                results = self.process_pymqi_response(obj_type, response)
                for result in results:
                    logger.debug(f"Output for {obj_type}: {json.dumps(result)}")
                    self._emit(result)

            except self.MQMIError as e:
                logger.error(f"Failed to process {obj_type}: Completion {e.comp}, Reason {e.reason}")
                print(json.dumps({
                    "queueManager": self.config['qmgr'],
                    "objectType": obj_type,
                    "completionCode": e.comp,
                    "reasonCode": e.reason,
                    "error": str(e)
                }, indent=2))
                sys.exit(1)
            except Exception as e:
                logger.error(f"Unexpected error for {obj_type}: {e}", exc_info=True)
                print(json.dumps({
                    "queueManager": self.config['qmgr'],
                    "objectType": obj_type,
                    "error": "Unexpected error occurred"
                }, indent=2))
                sys.exit(1)

    def close(self) -> None:
        if self.mode == 'rest':
            if self.session:
                self.session.close()
        elif self.mode == 'pymqi':
            if self.qmgr:
                self.qmgr.disconnect()

def load_config() -> Dict[str, str]:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Dump IBM MQ configuration as MQSC (default) or JSON using REST API or pymqi"
    )
    parser.add_argument(
        '--mode',
        default=os.getenv('MQ_MODE', MQConfigDumper.DEFAULT_CONFIG['mode']),
        choices=['rest', 'pymqi'],
        help="Mode: 'rest' (default) or 'pymqi'"
    )
    parser.add_argument(
        '--format',
        default=os.getenv('MQ_FORMAT', MQConfigDumper.DEFAULT_CONFIG['out_format']),
        choices=['mqsc', 'json'],
        help="Output format: 'mqsc' (default) or 'json'"
    )
    parser.add_argument(
        '-m', '--qmgr',
        default=os.getenv('MQ_QMGR', MQConfigDumper.DEFAULT_CONFIG['qmgr']),
        help="Queue manager name"
    )
    parser.add_argument(
        '-u', '--user',
        default=os.getenv('MQ_USER', MQConfigDumper.DEFAULT_CONFIG['user']),
        help="Username for authentication"
    )
    parser.add_argument(
        '-c', '--password',
        default=os.getenv('MQ_PASSWORD', MQConfigDumper.DEFAULT_CONFIG['password']),
        help="Password for authentication"
    )
    parser.add_argument(
        '-s', '--server',
        default=os.getenv('MQ_SERVER', MQConfigDumper.DEFAULT_CONFIG['server']),
        help="Server hostname for REST mode"
    )
    parser.add_argument(
        '-p', '--port',
        type=int,
        default=int(os.getenv('MQ_PORT', MQConfigDumper.DEFAULT_CONFIG['port'])),
        help="Server port for REST mode"
    )
    parser.add_argument(
        '--conn-type',
        default=os.getenv('MQ_CONN_TYPE', MQConfigDumper.DEFAULT_CONFIG['conn_type']),
        choices=['local', 'remote'],
        help="Connection type for pymqi mode: 'local' (default) or 'remote'"
    )
    parser.add_argument(
        '--host',
        default=os.getenv('MQ_HOST', MQConfigDumper.DEFAULT_CONFIG['host']),
        help="Host for remote pymqi connection"
    )
    parser.add_argument(
        '--conn-port',
        type=int,
        default=int(os.getenv('MQ_CONN_PORT', MQConfigDumper.DEFAULT_CONFIG['conn_port'])),
        help="Connection port for remote pymqi connection"
    )
    parser.add_argument(
        '--channel',
        default=os.getenv('MQ_CHANNEL', MQConfigDumper.DEFAULT_CONFIG['channel']),
        help="Channel for remote pymqi connection"
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help="Set logging level"
    )

    args = parser.parse_args()

    # Set logging level
    logger.setLevel(getattr(logging, args.log_level))

    return {
        'qmgr': args.qmgr,
        'user': args.user,
        'password': args.password,
        'server': args.server,
        'port': args.port,
        'timeout': MQConfigDumper.DEFAULT_CONFIG['timeout'],
        'mode': args.mode,
        'conn_type': args.conn_type,
        'host': args.host,
        'conn_port': args.conn_port,
        'channel': args.channel,
        'out_format': args.format,
    }

def main() -> None:
    config = load_config()
    dumper = MQConfigDumper(config)
    try:
        dumper.validate_config()
        dumper.dump_config()
    finally:
        dumper.close()

if __name__ == '__main__':
    main()
