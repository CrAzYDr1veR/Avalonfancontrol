#!/usr/bin/env python3
"""
Avalon Miner 1246 - Hybrid Fan Controller with Reboot + PSU Fault Detection
- Sets fan to 44% immediately on first contact with miner
- Below 78C : holds at 44% floor (no auto mode)
- 78-82C    : hysteresis gap - holds current speed
- Above 82C : steps fan to hold 80C target
- Detects reboots and logs bootby reason
- Detects PSU fault (error 2048) and alerts - requires manual power cycle
- Note: miner reports fan ~1% lower than commanded - 2% tolerance applied

Usage:
    python3 avalon_fan_control.py
    python3 avalon_fan_control.py --host 192.168.1.12 --port 4028
"""

import socket
import json
import re
import time
import argparse
import logging
from datetime import datetime

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

MINER_HOST      = "192.168.1.183"
MINER_PORT      = 4028

STARTUP_FAN     = 44     # % set immediately on first contact

TAKEOVER_TEMP   = 82     # C - start stepping fans up above this
HANDBACK_TEMP   = 78     # C - gap zone lower boundary

TARGET_TEMP     = 80     # C - TMax to hold when stepping
TOLERANCE       =  2     # C - dead band around target

STEP_UP         =  5     # % fan increase per cycle when too hot
STEP_DOWN       =  3     # % fan decrease per cycle when too cool

FAN_MIN         = 44     # % never go below this
FAN_MAX         = 100    # % ceiling

FAN_REPORT_TOLERANCE = 2  # % - miner reports ~1% low, ignore within this

PSU_FAULT_CODE  = 2048   # PS[0] value indicating PSU protection tripped

POLL_INTERVAL   = 20     # seconds between readings
SOCKET_TIMEOUT  = 10     # seconds

# ──────────────────────────────────────────────
# Logging - console + file
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("avalon_fan.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("fan_ctrl")

# ──────────────────────────────────────────────
# API
# ──────────────────────────────────────────────

def send_command(host, port, cmd):
    payload = json.dumps(cmd).encode()
    with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT) as s:
        s.sendall(payload)
        chunks = []
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks).decode(errors="replace")

def get_estats(host, port):
    return send_command(host, port, {"command": "estats"})

def get_bootby(host, port):
    return send_command(host, port, {"command": "ascset", "parameter": "0,bootby"})

def set_fan(host, port, speed):
    speed = max(FAN_MIN, min(FAN_MAX, int(speed)))
    resp = send_command(host, port, {"command": "ascset", "parameter": f"0,fan-spd,{speed}"})
    ok = "STATUS=S" in resp or "success" in resp.lower()
    return speed, ok

# ──────────────────────────────────────────────
# Parsing
# ──────────────────────────────────────────────

def parse_int(pattern, text):
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None

def parse_float(pattern, text):
    m = re.search(pattern, text)
    return float(m.group(1)) if m else None

def parse_psu_status(text):
    """Extract first value from PS[...] array - PSU status flag."""
    m = re.search(r'PS\[(\d+)', text)
    return int(m.group(1)) if m else None

def parse_bootby_code(text):
    m = re.search(r'BOOTBY\[([^\]]+)\]', text)
    if m:
        return m.group(1)
    m = re.search(r'bootby[,=:\s]+([0-9a-fx.]+)', text, re.IGNORECASE)
    if m:
        return m.group(1)
    return text.strip()[:120]

BOOTBY_CODES = {
    "0x00.00000000": "Normal power on",
    "0x01.00000000": "Watchdog reset",
    "0x02.00000000": "Software reboot (user initiated)",
    "0x03.00000000": "Exception / crash",
    "0x04.00000000": "Power loss recovery",
    "0x0a.0000001b": "Hashboard fault (CRC/comms error)",
    "0x0a.0000001e": "Hashboard fault / board missing",
    "0x0a.00000070": "Overheat protection",
}

def describe_bootby(code):
    return BOOTBY_CODES.get(code.lower(), "Unknown - check Avalon docs")

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main(host, port):
    log.info("=" * 55)
    log.info("Avalon 1246 fan controller")
    log.info("Startup fan: %d%%  |  Floor: %d%%  |  Ceiling: %d%%", STARTUP_FAN, FAN_MIN, FAN_MAX)
    log.info("Gap zone: %d-%dC  |  Target: %dC +/-%dC above %dC",
             HANDBACK_TEMP, TAKEOVER_TEMP, TARGET_TEMP, TOLERANCE, TAKEOVER_TEMP)
    log.info("Fan report tolerance: +/-%d%% (miner reports ~1%% low)", FAN_REPORT_TOLERANCE)
    log.info("PSU fault detection: enabled (flag=%d)", PSU_FAULT_CODE)
    log.info("Log file: avalon_fan.log")
    log.info("=" * 55)

    current_fan        = None
    consecutive_errors = 0
    miner_was_offline  = False
    last_elapsed       = None
    psu_fault_active   = False   # track so we don't spam the warning every cycle

    while True:
        time.sleep(POLL_INTERVAL)
        try:
            raw  = get_estats(host, port)
            consecutive_errors = 0

            tmax    = parse_int(r'TMax\[(\d+)\]', raw)
            tavg    = parse_int(r'TAvg\[(\d+)\]', raw)
            fanr    = parse_int(r'FanR\[(\d+)%\]', raw)
            ghs     = parse_float(r'GHSavg\[([0-9.]+)\]', raw)
            elapsed = parse_int(r'Elapsed=(\d+)', raw) or parse_int(r'Elapsed\[(\d+)\]', raw)
            crc0    = parse_int(r'CRC\[(\d+)', raw)
            psu     = parse_psu_status(raw)

            if tmax is None:
                log.warning("Could not parse TMax - skipping")
                continue

            # ── PSU fault detection ──────────────────────────

            if psu == PSU_FAULT_CODE:
                if not psu_fault_active:
                    psu_fault_active = True
                    log.critical("*" * 45)
                    log.critical("PSU FAULT DETECTED - PS flag = %d", psu)
                    log.critical("Boards have no power - miner cannot mine")
                    log.critical("*** MANUAL POWER CYCLE REQUIRED ***")
                    log.critical("Switch off power, wait 10 seconds, switch on")
                    log.critical("*" * 45)
                else:
                    log.warning("PSU fault still active (PS=%d) - waiting for power cycle", psu)
            else:
                if psu_fault_active:
                    log.info("PSU fault cleared (PS=%d) - power cycle successful", psu)
                    psu_fault_active = False

            # ── First contact ────────────────────────────────

            reboot_detected = False

            if current_fan is None:
                log.info("First contact - setting fan to %d%%", STARTUP_FAN)
                current_fan, ok = set_fan(host, port, STARTUP_FAN)
                log.info("Fan set to %d%%", current_fan)

            elif miner_was_offline:
                reboot_detected = True
                miner_was_offline = False
                log.warning("!" * 45)
                log.warning("MINER BACK ONLINE after being unreachable")

            elif last_elapsed is not None and elapsed is not None and elapsed < last_elapsed:
                reboot_detected = True
                log.warning("!" * 45)
                log.warning("REBOOT DETECTED - uptime reset %ds -> %ds", last_elapsed, elapsed)

            if reboot_detected:
                log.warning("Reboot at: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    bootby_resp = get_bootby(host, port)
                    code_estats = parse_bootby_code(raw)
                    code_query  = parse_bootby_code(bootby_resp)
                    log.warning("BOOTBY (estats): %s -> %s", code_estats, describe_bootby(code_estats))
                    log.warning("BOOTBY (query):  %s -> %s", code_query,  describe_bootby(code_query))
                except Exception as e:
                    log.warning("Could not query bootby: %s", e)
                log.warning("!" * 45)
                log.info("Setting fan to %d%% after reboot", STARTUP_FAN)
                current_fan, _ = set_fan(host, port, STARTUP_FAN)

            last_elapsed = elapsed

            # ── CRC warning ──────────────────────────────────

            if crc0 and crc0 > 0:
                log.warning("CRC errors on board 0: %d - board comms fault!", crc0)

            # ── Status log ───────────────────────────────────

            fan_ok = fanr is not None and abs(fanr - current_fan) <= FAN_REPORT_TOLERANCE
            fan_status = "ok" if fan_ok else f"MISMATCH reported={fanr}%"
            psu_str = f"PSU=FAULT({psu})" if psu == PSU_FAULT_CODE else f"PSU=ok({psu})"

            log.info("TMax=%dC  TAvg=%sC  FanR=%s%%(%s)  cmd=%d%%  GHS=%s  %s  uptime=%ss",
                     tmax, tavg, fanr, fan_status, current_fan, ghs, psu_str, elapsed)

            # ── Fan control (skip if PSU fault - boards have no power) ───

            if psu_fault_active:
                log.info("  Fan control paused - PSU fault active")
                continue

            if tmax > TAKEOVER_TEMP:
                diff = tmax - TARGET_TEMP
                if diff > TOLERANCE:
                    new_fan = current_fan + STEP_UP
                    log.info("  Too hot (+%dC above target) - fan %d%% -> %d%%",
                             diff, current_fan, new_fan)
                    current_fan, ok = set_fan(host, port, new_fan)
                    if not ok:
                        log.warning("  Fan command may have failed")
                elif diff < -TOLERANCE:
                    new_fan = current_fan - STEP_DOWN
                    log.info("  Too cool (%dC below target) - fan %d%% -> %d%%",
                             abs(diff), current_fan, new_fan)
                    current_fan, ok = set_fan(host, port, new_fan)
                    if not ok:
                        log.warning("  Fan command may have failed")
                else:
                    log.info("  On target (%dC +/-%dC) - holding %d%%",
                             TARGET_TEMP, TOLERANCE, current_fan)

            elif tmax >= HANDBACK_TEMP:
                log.info("  Gap zone (%d-%dC) - holding %d%%",
                         HANDBACK_TEMP, TAKEOVER_TEMP, current_fan)

            else:
                if current_fan != FAN_MIN:
                    log.info("  Cool (%dC) - stepping back to floor %d%%", tmax, FAN_MIN)
                    current_fan, _ = set_fan(host, port, FAN_MIN)
                else:
                    log.info("  Cool (%dC) - holding floor %d%%", tmax, FAN_MIN)

        except (OSError, socket.timeout) as e:
            consecutive_errors += 1
            miner_was_offline = True
            if consecutive_errors == 1:
                log.warning("Miner went offline at: %s",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log.error("Network error #%d: %s", consecutive_errors, e)
            if consecutive_errors >= 5:
                log.critical("5 consecutive errors - miner still offline. Last fan: %s%%", current_fan)

        except Exception as e:
            log.exception("Unexpected error: %s", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Avalon 1246 fan controller")
    parser.add_argument("--host", default=MINER_HOST)
    parser.add_argument("--port", type=int, default=MINER_PORT)
    args = parser.parse_args()
    main(args.host, args.port)
