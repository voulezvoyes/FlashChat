"""
SafeCrew Live - 백엔드 서버
------------------------------------
- 세션(방)은 전부 메모리(dict)에만 저장됨. 서버 재시작하면 다 사라짐 (Zero-Storage 컨셉과 일치).
- 위치/상태는 WebSocket으로 실시간 브로드캐스트.
- 낙오(LAGGING) = 호스트(리더) 기준 300m 이상 벌어짐
- 정지(STUCK) = 90초간 5m 이상 이동 없음
- EMERGENCY = 멤버가 직접 SOS 버튼 누른 경우 (수동, 최우선)
"""

import asyncio
import json
import math
import os
import random
import re
import string
import time
from typing import Dict, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

load_dotenv()

app = FastAPI()

# 향후 "누적 마일/스트릭" 기능에 관심있는 유저 이메일만 가볍게 수집 (실제 계정/DB 아님)
interest_emails: list = []


@app.post("/api/interest")
async def register_interest(payload: dict):
    email = (payload.get("email") or "").strip()
    if "@" in email and "." in email.split("@")[-1]:
        interest_emails.append(email)
        return {"ok": True}
    return {"ok": False}

# ─────────────────────────────────────────────
# SodaGift (스폰서 기프팅 API) 연동
# ─────────────────────────────────────────────
SODAGIFT_API_KEY = os.getenv("SODAGIFT_API_KEY", "")
SODAGIFT_BASE_URL = "https://biz-sandbox-api.sodagift.com"  # 샌드박스. 실제 발급 땐 biz-api.sodagift.com

_cached_product_id: Optional[int] = None


async def find_gift_product_id(client: httpx.AsyncClient, headers: dict) -> Optional[int]:
    """CODE 배송 가능한 상품 중 하나를 찾아서 캐싱 (매번 카탈로그 조회 안 하려고)."""
    global _cached_product_id
    if _cached_product_id:
        return _cached_product_id
    resp = await client.get(
        f"{SODAGIFT_BASE_URL}/v1/products",
        headers=headers,
        params={"page": 0, "size": 50, "delivery_method": "CODE"},
    )
    if resp.status_code != 200:
        print("SodaGift product lookup failed:", resp.status_code, resp.text)
        return None
    products = resp.json().get("products", [])
    candidates = [p for p in products if p.get("availability") == "ON_SALE"]
    if not candidates:
        return None
    # 제일 저렴한 걸로 (amount 고정가 상품 우선, 없으면 min_amount 기준)
    candidates.sort(key=lambda p: p.get("amount") or p.get("min_amount") or 999999)
    _cached_product_id = candidates[0]["id"]
    return _cached_product_id


async def issue_gift(session_id: str, recipient_name: str) -> Optional[dict]:
    """MVP한테 줄 기프트 코드를 실제로 발급. 실패하면 None 리턴 (데모가 이거 때문에 안 죽게)."""
    if not SODAGIFT_API_KEY:
        print("SODAGIFT_API_KEY not set — skipping real gift issuance")
        return None
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            headers = {"SODA-API-KEY": SODAGIFT_API_KEY}
            product_id = await find_gift_product_id(client, headers)
            if not product_id:
                return None

            # external_reference_id는 영숫자만 허용 (문서 스펙) -> 특수문자 다 제거
            raw_ref = f"safecrew{session_id}{recipient_name}{int(time.time())}"
            ext_ref = re.sub(r"[^a-zA-Z0-9]", "", raw_ref)[:100]

            order_resp = await client.post(
                f"{SODAGIFT_BASE_URL}/v1/orders",
                headers=headers,
                json={
                    "item": {"id": product_id},
                    "delivery": {"method": "CODE"},
                    "note": "SafeCrew Live MVP reward",
                    "external_reference_id": ext_ref,
                },
            )
            if order_resp.status_code != 200:
                print("SodaGift order failed:", order_resp.status_code, order_resp.text)
                return None

            order_id = order_resp.json()["id"]

            # CODE는 생성 응답에 안 실려있어서 상세조회로 한 번 더 가져와야 함
            detail_resp = await client.get(f"{SODAGIFT_BASE_URL}/v1/orders/{order_id}", headers=headers)
            if detail_resp.status_code == 200:
                items = detail_resp.json().get("order_items", [])
                if items:
                    code = (items[0].get("delivery") or {}).get("code") or {}
                    if code.get("value"):
                        return {"value": code.get("value"), "pin": code.get("pin"), "url": code.get("url")}
            return {"pending": True}  # 주문은 됐는데 코드가 아직 발급 중인 경우
    except Exception as e:
        print("SodaGift integration error:", repr(e))
        return None


# ── 새 흐름: 자동발급 대신, MVP 본인이 카탈로그 보고 골라서 이메일로 받기 ──
GIFT_MAX_AMOUNT = 20000  # 통화 무관 숫자 비교라 넉넉하게 (원화면 숫자가 훨씬 크기 때문). 카탈로그 확인 후 낮춰도 됨


@app.get("/api/gift_catalog")
async def get_gift_catalog():
    """MVP가 고를 수 있는 상품 목록. GIFT_MAX_AMOUNT 이하, EMAIL 배송 가능한 것만
    (통화는 더 이상 USD로 제한 안 함 — 통화별로 표시)."""
    if not SODAGIFT_API_KEY:
        print("gift_catalog: SODAGIFT_API_KEY 없음")
        return {"products": []}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            headers = {"SODA-API-KEY": SODAGIFT_API_KEY}
            resp = await client.get(
                f"{SODAGIFT_BASE_URL}/v1/products",
                headers=headers,
                params={"page": 0, "size": 50, "delivery_method": "EMAIL"},
            )
            if resp.status_code != 200:
                print("gift_catalog fetch failed:", resp.status_code, resp.text)
                return {"products": []}
            raw = resp.json().get("products", [])
            on_sale = [p for p in raw if p.get("availability") == "ON_SALE"]
            within_budget = [p for p in on_sale if (p.get("amount") is not None) and p["amount"] <= GIFT_MAX_AMOUNT]
            # 디버깅용: 어느 단계에서 다 걸러지는지 서버 터미널에 그대로 보이게 함
            print(f"gift_catalog: EMAIL배송 상품 {len(raw)}개 -> ON_SALE {len(on_sale)}개 -> ${GIFT_MAX_AMOUNT}이하 {len(within_budget)}개")

            out = []
            for p in within_budget:
                out.append({
                    "id": p["id"],
                    "name": p.get("name"),
                    "amount": p.get("amount"),
                    "currency": p.get("currency"),
                    "brand": (p.get("brand") or {}).get("name") or p.get("name"),
                    "image_url": p.get("image_url"),
                })
            return {"products": out[:12]}
    except Exception as e:
        print("gift_catalog error:", repr(e))
        return {"products": []}


@app.post("/api/claim_gift")
async def claim_gift(payload: dict):
    """MVP가 카탈로그에서 고른 상품 + 이메일로 실제 주문 생성 (EMAIL 배송)."""
    product_id = payload.get("product_id")
    email = (payload.get("email") or "").strip()
    recipient_name = (payload.get("name") or "MVP").strip()
    if not product_id or "@" not in email:
        return {"ok": False, "error": "invalid_request"}
    if not SODAGIFT_API_KEY:
        return {"ok": False, "error": "no_api_key"}
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            headers = {"SODA-API-KEY": SODAGIFT_API_KEY}
            ext_ref = re.sub(r"[^a-zA-Z0-9]", "", f"microthonclaim{int(time.time())}{email}")[:100]
            order_resp = await client.post(
                f"{SODAGIFT_BASE_URL}/v1/orders",
                headers=headers,
                json={
                    "item": {"id": int(product_id)},
                    "delivery": {
                        "method": "EMAIL",
                        "recipient": {"name": recipient_name, "email": email},
                        "sender": {"name": "Microthon"},
                    },
                    "note": "Microthon MVP reward",
                    "external_reference_id": ext_ref,
                },
            )
            if order_resp.status_code != 200:
                print("claim_gift order failed:", order_resp.status_code, order_resp.text)
                detail = order_resp.text
                try:
                    body = order_resp.json()
                    detail = f'{body.get("errorCode", "")}: {body.get("message", "")}'.strip(": ")
                except Exception:
                    pass
                return {"ok": False, "error": "order_failed", "detail": detail}
            return {"ok": True}
    except Exception as e:
        print("claim_gift error:", repr(e))
        return {"ok": False, "error": "exception", "detail": str(e)}


# ─────────────────────────────────────────────
# 출발지/목적지 설정 (Nominatim 무료 지오코딩 - 가입/키 불필요, 초당 1회 제한만 지킴)
# ─────────────────────────────────────────────
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


async def geocode(query: str):
    """장소 이름 -> (lat, lng, 실제매칭된주소). 못 찾으면 None."""
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(
                NOMINATIM_URL,
                params={
                    "q": query, "format": "json", "limit": 1,
                    "viewbox": "-118.6,34.35,-118.1,33.9",  # LA 지역 결과를 우선순위로 (완전 차단은 아님)
                },
                headers={"User-Agent": "MicrothonHackathonDemo/1.0"},  # Nominatim 정책상 필수
            )
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"]), data[0].get("display_name", query)
    except Exception as e:
        print("geocode error:", repr(e))
    return None


OSRM_URL = "https://router.project-osrm.org/route/v1/foot"  # FOSSGIS 후원 공개 데모서버, foot 프로필 지원


async def get_road_route(start, dest):
    """실제 도로/보행로를 따라가는 좌표들을 받아옴. 실패하면 None (호출부에서 직선으로 폴백)."""
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            # OSRM은 "경도,위도" 순서 (Leaflet은 반대인 "위도,경도"라 헷갈리기 쉬움)
            coords = f"{start[1]},{start[0]};{dest[1]},{dest[0]}"
            resp = await client.get(
                f"{OSRM_URL}/{coords}",
                params={"geometries": "geojson", "overview": "full"},
            )
            if resp.status_code != 200:
                print(f"OSRM HTTP 에러: {resp.status_code} {resp.text[:200]}")
                return None
            data = resp.json()
            if data.get("code") == "Ok" and data.get("routes"):
                coords_lnglat = data["routes"][0]["geometry"]["coordinates"]
                print(f"OSRM 라우팅 성공: {len(coords_lnglat)}개 좌표")
                return [[lat, lng] for lng, lat in coords_lnglat]  # Leaflet 순서로 변환
            print(f"OSRM 라우팅 실패, code={data.get('code')} (직선으로 폴백함)")
    except Exception as e:
        print("OSRM routing error:", repr(e))
    return None


@app.post("/api/set_route/{session_id}")
async def set_route(session_id: str, payload: dict):
    room = rooms.get(session_id)
    if not room:
        return {"ok": False, "error": "session_not_found"}

    start_coords = payload.get("start_coords")  # 지도 탭으로 직접 찍은 경우 [lat, lng]
    dest_coords = payload.get("dest_coords")

    if start_coords and dest_coords:
        # 지오코딩 생략, 탭한 좌표 그대로 사용 (검색어 문제 원천 차단)
        start_lat, start_lng = start_coords
        dest_lat, dest_lng = dest_coords
        start_label = payload.get("start_label") or "Start point"
        dest_label = payload.get("dest_label") or "Destination point"
        start_matched, dest_matched = start_label, dest_label
    else:
        start_q = (payload.get("start") or "").strip()
        dest_q = (payload.get("destination") or "").strip()
        if not start_q or not dest_q:
            return {"ok": False, "error": "missing_fields"}
        start_result = await geocode(start_q)
        dest_result = await geocode(dest_q)
        if not start_result or not dest_result:
            return {"ok": False, "error": "geocode_failed"}
        start_lat, start_lng, start_matched = start_result
        dest_lat, dest_lng, dest_matched = dest_result
        start_label, dest_label = start_q, dest_q

    road_route = await get_road_route((start_lat, start_lng), (dest_lat, dest_lng))
    route = road_route if road_route else [[start_lat, start_lng], [dest_lat, dest_lng]]  # OSRM 실패시 직선으로라도 보여줌

    room["route"] = route
    room["route_labels"] = [start_label, dest_label]
    await broadcast(session_id)  # 다들 지도에 새 경로 뜨게
    return {
        "ok": True,
        "route_points": len(route),
        "route": route,
        "used_road_routing": road_route is not None,
        "route_labels": room["route_labels"],
        "matched": [start_matched, dest_matched],  # 실제로 뭘 찾았는지 호스트가 확인할 수 있게
    }

# ─────────────────────────────────────────────
# 메모리 저장소 (Redis 아니고 그냥 파이썬 dict.
# 해커톤 데모용으론 이걸로 충분함. 서버 프로세스 하나만 쓸 거라 문제 없음)
# ─────────────────────────────────────────────
rooms: Dict[str, dict] = {}

STUCK_SECONDS = 90       # 이 시간 이상 안 움직이면 정지 판정
STUCK_RADIUS_M = 5       # 이 반경 안에서만 움직이면 "안 움직인 것"으로 침
LAG_DISTANCE_M = 300     # 리더 기준 이 거리 이상 벌어지면 낙오 판정
GPS_ACCURACY_LIMIT = 30  # 오차(m)가 이보다 크면 그 위치값은 무시 (GPS 튐 필터)

FUN_NICKNAMES = ["Tiger", "Falcon", "Rocket", "Comet", "Panda", "Phoenix", "Cobra", "Shark"]


def new_session_id() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def haversine(lat1, lng1, lat2, lng2) -> float:
    """두 좌표 사이 거리(미터)"""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


DEFAULT_ROUTE = [
    [34.0224, -118.2851], [34.0400, -118.2915], [34.0600, -118.2952],
    [34.0928, -118.2998], [34.1050, -118.3080], [34.1180, -118.3160], [34.1341, -118.3215]
]  # 기본값: USC -> 할리우드 사인 (호스트가 커스텀 경로 안 정하면 이거)
DEFAULT_ROUTE_LABELS = ["USC", "Hollywood Sign"]


def get_room(session_id: str) -> dict:
    if session_id not in rooms:
        rooms[session_id] = {
            "members": {},       # member_id -> member dict
            "leader_pos": None,  # (lat, lng) - 호스트 위치 기준
            "phase": "WAITING",  # WAITING -> RUNNING -> VOTING -> ENDED
            "route": DEFAULT_ROUTE,
            "route_labels": DEFAULT_ROUTE_LABELS,
        }
    return rooms[session_id]


def public_member_view(m: dict) -> dict:
    """프론트에 보낼 때 필요한 필드만 추려서 전송"""
    return {
        "id": m["id"],
        "name": m["name"],
        "role": m["role"],
        "lat": m["lat"],
        "lng": m["lng"],
        "status": m["status"],          # NORMAL / LAGGING / STUCK / EMERGENCY
        "self_status": m["self_status"],  # OK / WATER_BREAK / REST / HOME
        "connected": m["connected"],
        "sim": m.get("sim", False),      # 시뮬레이터 봇 여부 (UI에 SIM 배지 표시용)
    }


async def broadcast(session_id: str):
    room = rooms.get(session_id)
    if not room:
        return
    payload = json.dumps({
        "type": "CREW_UPDATE",
        "phase": room["phase"],
        "members": [public_member_view(m) for m in room["members"].values()],
        "route": room.get("route", DEFAULT_ROUTE),
        "route_labels": room.get("route_labels", DEFAULT_ROUTE_LABELS),
    })
    dead = []
    for mid, ws in room.get("connections", {}).items():
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(mid)
    for mid in dead:
        room["connections"].pop(mid, None)


async def broadcast_chat(session_id: str, name: str, text: str):
    room = rooms.get(session_id)
    if not room:
        return
    payload = json.dumps({"type": "CHAT", "name": name, "text": text, "ts": time.time()})
    for ws in list(room.get("connections", {}).values()):
        try:
            await ws.send_text(payload)
        except Exception:
            pass


def compute_mvp_fallback(room: dict):
    """아무도 투표 안 했을 때 쓰는 백업 규칙: 낙오/정지 횟수 제일 적은 사람.
    실제 알고리즘 아니고 데모용 하드코딩 룰. 멤버가 호스트 혼자뿐(솔로 테스트)이면
    호스트도 후보에 넣어서 항상 누군가는 뽑히게 한다."""
    candidates = [m for m in room["members"].values() if m["role"] != "host"]
    if not candidates:
        candidates = list(room["members"].values())
    if not candidates:
        return None
    candidates.sort(key=lambda m: (m["stuck_count"] + m["lagging_count"], m["name"]))
    return candidates[0]["name"]


VOTE_TIMEOUT_SECONDS = 20  # 이 시간 지나면 투표 안 한 사람 있어도 그냥 마감


async def start_voting(session_id: str):
    room = rooms.get(session_id)
    if not room:
        return
    room["phase"] = "VOTING"
    room["votes"] = {}
    room["voting_concluded"] = False
    await broadcast(session_id)
    asyncio.create_task(voting_timeout(session_id))


async def voting_timeout(session_id: str):
    await asyncio.sleep(VOTE_TIMEOUT_SECONDS)
    await conclude_voting(session_id)


async def conclude_voting(session_id: str):
    room = rooms.get(session_id)
    if not room or room.get("voting_concluded"):
        return  # 이미 마감됐으면 중복 실행 방지 (타임아웃이랑 전원투표완료가 동시에 걸릴 수 있음)
    room["voting_concluded"] = True
    for task in room.get("sim_tasks", []):  # 러닝 끝났으니 시뮬레이터 봇들도 정지
        task.cancel()
    room["sim_tasks"] = []

    tally: Dict[str, int] = {}
    for target in room.get("votes", {}).values():
        tally[target] = tally.get(target, 0) + 1

    if tally:
        top = max(tally.values())
        winners = sorted(name for name, count in tally.items() if count == top)
        mvp = winners[0]  # 동점이면 이름 사전순으로 정함 (데모용 단순 처리)
    else:
        mvp = compute_mvp_fallback(room)  # 아무도 투표 안 하면 백업 규칙 사용

    room["phase"] = "ENDED"
    # 이제 자동발급 안 함 — MVP 본인이 결과화면에서 카탈로그 보고 골라서 이메일로 받음 (/api/claim_gift)
    for member_id, ws2 in list(room.get("connections", {}).items()):
        is_winner = room["members"].get(member_id, {}).get("name") == mvp
        payload = json.dumps({
            "type": "SESSION_END",
            "mvp": mvp,
            "tally": tally,
            "can_claim_gift": is_winner and bool(mvp),
        })
        try:
            await ws2.send_text(payload)
        except Exception:
            pass
    await broadcast(session_id)


# ─────────────────────────────────────────────
# REST API
# ─────────────────────────────────────────────
@app.post("/api/create_session")
async def create_session():
    sid = new_session_id()
    get_room(sid)
    return {"session_id": sid}


@app.get("/api/random_nickname")
async def random_nickname():
    return {"nickname": random.choice(FUN_NICKNAMES) + str(random.randint(1, 99))}


# ─────────────────────────────────────────────
# 시뮬레이터 (터미널 스크립트 대신 API로 호출 - UI 버튼에서 바로 트리거)
# ─────────────────────────────────────────────
SIM_PROFILES = {
    "fast":   {"speed": 0.08, "stuck_after": None, "stuck_duration": 0},
    "normal": {"speed": 0.05, "stuck_after": None, "stuck_duration": 0},
    "slow":   {"speed": 0.018, "stuck_after": None, "stuck_duration": 0},
    "stuck":  {"speed": 0.04, "stuck_after": 15, "stuck_duration": 100},
}
SIM_NAMES = ["SimSprinter", "SimRunner", "SimSlowpoke", "SimStuck", "Sim5", "Sim6", "Sim7", "Sim8", "Sim9"]


async def run_sim_bot(session_id: str, bot_id: str, profile_key: str, elapsed_start: float):
    """서버 프로세스 안에서 직접 도는 가상 러너 - 진짜 WebSocket 연결 없이 room 안의
    좌표만 route를 따라 이동시키고 broadcast()로 다른 사람들한테 보이게 함."""
    profile = SIM_PROFILES.get(profile_key, SIM_PROFILES["normal"])
    room = rooms.get(session_id)
    if not room:
        return
    route = room.get("route", DEFAULT_ROUTE)
    t = 0.0
    stuck_until = None
    try:
        while True:
            await asyncio.sleep(2)
            room = rooms.get(session_id)
            if not room or bot_id not in room["members"]:
                return  # 세션 없어졌거나 봇이 강제로 빠짐
            if room["phase"] not in ("RUNNING",):
                continue  # 러닝 시작 전엔 제자리

            elapsed = time.time() - elapsed_start
            if profile["stuck_after"] and elapsed >= profile["stuck_after"] and stuck_until is None:
                stuck_until = elapsed + profile["stuck_duration"]
            if stuck_until and elapsed < stuck_until:
                pass  # 멈춘 상태 유지 (t 증가 안 시킴 -> STUCK 판정 자연 발생)
            else:
                t = min(1.0, t + profile["speed"] * 0.08)

            idx_f = t * (len(route) - 1)
            idx = min(int(idx_f), len(route) - 2) if len(route) > 1 else 0
            frac = idx_f - idx
            lat = route[idx][0] + (route[idx+1][0] - route[idx][0]) * frac
            lng = route[idx][1] + (route[idx+1][1] - route[idx][1]) * frac

            member = room["members"].get(bot_id)
            if not member:
                return
            now = time.time()
            d = haversine(member["last_pos"][0], member["last_pos"][1], lat, lng) if member.get("last_pos") else 999
            if d > STUCK_RADIUS_M:
                member["last_moved_ts"] = now
            member["lat"], member["lng"], member["last_pos"] = lat, lng, (lat, lng)

            leader = room.get("leader_pos")
            if leader:
                dist_from_leader = haversine(leader[0], leader[1], lat, lng)
                member["status"] = "LAGGING" if dist_from_leader > LAG_DISTANCE_M else "NORMAL"
            if now - member["last_moved_ts"] > STUCK_SECONDS:
                member["status"] = "STUCK"

            await broadcast(session_id)
    except asyncio.CancelledError:
        return


@app.post("/api/spawn_sim_runners/{session_id}")
async def spawn_sim_runners(session_id: str, payload: dict):
    """페이스 다양한 가짜 러너들을 즉석에서 room에 투입 (실제 WS연결 없이 서버가 직접 움직임)."""
    room = rooms.get(session_id)
    if not room:
        return {"ok": False, "error": "session_not_found"}
    count = max(1, min(int(payload.get("count", 4)), 9))
    profiles_cycle = ["fast", "normal", "slow", "stuck"]
    now = time.time()
    spawned = []
    room.setdefault("sim_tasks", [])
    for i in range(count):
        bot_id = SIM_NAMES[i] if i < len(SIM_NAMES) else f"Sim{i}"
        if bot_id in room["members"]:
            continue  # 이미 떠있으면 중복 투입 안 함
        profile_key = profiles_cycle[i % len(profiles_cycle)]
        room["members"][bot_id] = {
            "id": bot_id, "name": bot_id, "role": "member",
            "lat": None, "lng": None, "status": "NORMAL", "self_status": "OK",
            "last_moved_ts": now, "last_pos": None, "connected": True,
            "stuck_count": 0, "lagging_count": 0, "sim": True,
        }
        task = asyncio.create_task(run_sim_bot(session_id, bot_id, profile_key, now))
        room["sim_tasks"].append(task)
        spawned.append(bot_id)
    await broadcast(session_id)
    return {"ok": True, "spawned": spawned}


@app.post("/api/remove_sim_runners/{session_id}")
async def remove_sim_runners(session_id: str):
    """시뮬레이터 봇들 정리 (태스크 취소 + 크루목록에서 제거)."""
    room = rooms.get(session_id)
    if not room:
        return {"ok": False, "error": "session_not_found"}
    for task in room.get("sim_tasks", []):
        task.cancel()
    room["sim_tasks"] = []
    removed = [mid for mid, m in list(room["members"].items()) if m.get("sim")]
    for mid in removed:
        del room["members"][mid]
        room.get("connections", {}).pop(mid, None)
    await broadcast(session_id)
    return {"ok": True, "removed": removed}


# ─────────────────────────────────────────────
# WebSocket
# ─────────────────────────────────────────────
@app.websocket("/ws/{session_id}")
async def ws_endpoint(websocket: WebSocket, session_id: str, name: str, role: str = "member"):
    await websocket.accept()
    room = get_room(session_id)

    # 닉네임 중복 방지
    member_id = name
    if member_id in room["members"] and room["members"][member_id]["connected"]:
        member_id = f"{name}_{random.randint(10, 99)}"

    room["members"][member_id] = {
        "id": member_id,
        "name": name,
        "role": role,               # "host" or "member"
        "lat": None,
        "lng": None,
        "status": "NORMAL",
        "self_status": "OK",
        "last_moved_ts": time.time(),
        "last_pos": None,
        "connected": True,
        "stuck_count": 0,
        "lagging_count": 0,
    }
    room.setdefault("connections", {})[member_id] = websocket

    try:
        await broadcast(session_id)
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            member = room["members"][member_id]
            action = msg.get("action")

            if action == "LOCATION":
                lat, lng, accuracy = msg.get("lat"), msg.get("lng"), msg.get("accuracy", 0)
                if accuracy and accuracy > GPS_ACCURACY_LIMIT:
                    continue  # GPS 튐 필터

                now = time.time()
                if member["last_pos"]:
                    moved = haversine(member["last_pos"][0], member["last_pos"][1], lat, lng)
                    if moved > STUCK_RADIUS_M:
                        member["last_moved_ts"] = now
                member["last_pos"] = (lat, lng)
                member["lat"], member["lng"] = lat, lng

                if role == "host":
                    room["leader_pos"] = (lat, lng)

                prev_status = member["status"]
                if member["status"] != "EMERGENCY":  # SOS 걸려있으면 자동판정으로 안 덮어씀
                    stationary_secs = now - member["last_moved_ts"]
                    if stationary_secs >= STUCK_SECONDS:
                        member["status"] = "STUCK"
                    elif room["leader_pos"]:
                        d = haversine(room["leader_pos"][0], room["leader_pos"][1], lat, lng)
                        member["status"] = "LAGGING" if d >= LAG_DISTANCE_M else "NORMAL"
                    else:
                        member["status"] = "NORMAL"

                    # 상태가 새로 STUCK/LAGGING 으로 "전환"된 순간만 카운트 (계속 눌려있는 동안 중복 카운트 방지)
                    if member["status"] == "STUCK" and prev_status != "STUCK":
                        member["stuck_count"] += 1
                    if member["status"] == "LAGGING" and prev_status != "LAGGING":
                        member["lagging_count"] += 1

                await broadcast(session_id)

            elif action == "STATUS":
                value = msg.get("value", "OK")
                if value == "EMERGENCY":
                    member["status"] = "EMERGENCY"
                    member["self_status"] = "OK"
                elif value in ("OK", "CLEAR_EMERGENCY"):
                    # "I'm OK" 버튼: SOS로 빨갛게 고정돼있던 걸 정상으로 되돌림
                    if member["status"] == "EMERGENCY":
                        member["status"] = "NORMAL"
                    member["self_status"] = "OK"
                else:
                    member["self_status"] = value  # WATER_BREAK / REST / HOME
                await broadcast(session_id)

            elif action == "CHAT":
                await broadcast_chat(session_id, member["name"], msg.get("text", ""))

            elif action == "START_SESSION" and role == "host":
                room["phase"] = "RUNNING"
                await broadcast(session_id)

            elif action == "END_SESSION" and role == "host":
                await start_voting(session_id)

            elif action == "VOTE" and room["phase"] == "VOTING":
                target = msg.get("target")
                if target:
                    room.setdefault("votes", {})[member_id] = target
                    # 접속해 있는 사람 전원이 투표를 마치면 타임아웃 기다릴 필요 없이 바로 마감
                    eligible_voters = [m for m in room["members"].values() if m["connected"]]
                    if len(room["votes"]) >= len(eligible_voters):
                        await conclude_voting(session_id)

    except WebSocketDisconnect:
        if member_id in room["members"]:
            room["members"][member_id]["connected"] = False
        if member_id in room.get("connections", {}):
            room["connections"].pop(member_id, None)
        await broadcast(session_id)


app.mount("/", StaticFiles(directory="static", html=True), name="static")
