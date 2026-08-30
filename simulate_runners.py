"""
SafeCrew Live - 가짜 러너 시뮬레이터
------------------------------------------
실제 폰 여러 대 없이도 낙오(LAGGING)/정지(STUCK)/여러명 투표를 테스트하거나
데모 중에 "다른 러너들"을 화면에 띄우고 싶을 때 쓰는 스크립트.

사용법:
    1. 서버(uvicorn)를 먼저 켜놓는다.
    2. 브라우저에서 host.html 열어서 세션 코드 확인 (예: AB12CD).
    3. 새 터미널 창에서:
       python simulate_runners.py AB12CD

    그럼 가짜 러너 3명이 자동으로 접속해서 USC->할리우드 사인 경로를 따라
    움직이기 시작한다. 그중 SimSlowpoke는 일부러 천천히 가서 낙오(주황)를
    만들고, SimStuck은 중간에 멈춰서 정지(빨강)를 만들어서 안전기능 데모가
    바로 됨.

    호스트 화면은 계속 브라우저에서 열어둔 채로 실제 위치추적이 되고 있어야
    "리더 기준 낙오 거리"가 계산된다 (호스트 위치가 없으면 낙오 판정 자체가 안 됨).
"""

import asyncio
import json
import random
import sys

import websockets

ROUTE_WAYPOINTS = [
    (34.0224, -118.2851),  # USC
    (34.0400, -118.2915),
    (34.0600, -118.2952),
    (34.0928, -118.2998),
    (34.1050, -118.3080),
    (34.1180, -118.3160),
    (34.1341, -118.3215),  # Hollywood Sign
]


def interpolate(progress: float):
    """progress: 0.0 ~ len(waypoints)-1 사이 실수. 두 웨이포인트 사이를 선형보간."""
    idx = min(int(progress), len(ROUTE_WAYPOINTS) - 2)
    frac = progress - idx
    lat1, lng1 = ROUTE_WAYPOINTS[idx]
    lat2, lng2 = ROUTE_WAYPOINTS[idx + 1]
    return lat1 + (lat2 - lat1) * frac, lng1 + (lng2 - lng1) * frac


async def run_bot(uri: str, name: str, speed: float, stuck_after: float = None, stuck_duration: float = 0):
    max_progress = len(ROUTE_WAYPOINTS) - 1
    progress = 0.0
    elapsed = 0.0
    tick = 3  # 3초마다 위치 전송 (실제 GPS 폴링 주기랑 비슷하게)

    async with websockets.connect(uri) as ws:
        print(f"[{name}] 접속 완료, 이동 시작")
        while progress < max_progress:
            is_stuck_now = stuck_after is not None and stuck_after <= elapsed < stuck_after + stuck_duration
            if not is_stuck_now:
                progress = min(progress + speed, max_progress)

            lat, lng = interpolate(progress)
            # GPS 튐 느낌 나게 살짝 노이즈
            lat += random.uniform(-0.00015, 0.00015)
            lng += random.uniform(-0.00015, 0.00015)

            await ws.send(json.dumps({"action": "LOCATION", "lat": lat, "lng": lng, "accuracy": 8}))
            tag = " (STUCK 중)" if is_stuck_now else ""
            print(f"[{name}] progress={progress:.2f}/{max_progress}{tag}")

            await asyncio.sleep(tick)
            elapsed += tick
        print(f"[{name}] 완주!")


async def main():
    if len(sys.argv) < 2:
        print("사용법: python simulate_runners.py <세션코드>")
        print("예:     python simulate_runners.py AB12CD")
        return

    session_id = sys.argv[1].upper()
    host = sys.argv[2] if len(sys.argv) > 2 else "localhost:8000"  # 포트가 다르면 두번째 인자로 넘기면 됨
    base = f"ws://{host}/ws/{session_id}"

    bots = [
        # 빠른 페이스로 크루 맨 앞에서 뛰는 러너
        run_bot(f"{base}?name=SimSprinter&role=member", "SimSprinter", speed=0.08),
        # 정상 페이스로 완주하는 러너
        run_bot(f"{base}?name=SimRunner&role=member", "SimRunner", speed=0.05),
        # 페이스가 느려서 결국 리더 기준 300m 이상 벌어지는 러너 -> LAGGING(주황) 데모용
        run_bot(f"{base}?name=SimSlowpoke&role=member", "SimSlowpoke", speed=0.018),
        # 15초 뛰다가 100초간 멈추는 러너 -> STUCK(빨강) 데모용 (정지 기준 90초)
        run_bot(f"{base}?name=SimStuck&role=member", "SimStuck", speed=0.04, stuck_after=15, stuck_duration=100),
    ]

    print(f"세션 {session_id}에 가짜 러너 {len(bots)}명 투입. Ctrl+C로 중단 가능.")
    await asyncio.gather(*bots)


if __name__ == "__main__":
    asyncio.run(main())
