# 2602_money

한국 주식 시장을 1시간 단위로 스캔해 자금 유입 가능성이 높은 후보를 선정하고, 결과를 텔레그램으로 전송하는 리서치 자동화 도구입니다.

## 주의
이 프로젝트는 매수/매도 자문이 아닌 분석 자동화입니다. 실제 투자 의사결정/실행은 사용자 책임입니다.

## 구조
- `src/providers`: 데이터 소스 플러그인(KIS/FDR/pykrx)
- `src/features`: 피처 계산
- `src/scoring`: 스코어 계산/가중치
- `src/analysis`: 고급 해설(옵션, Ollama + Gemma 12B)
- `src/events`: 뉴스/이벤트 리스크 스코어링
- `src/feedback`: 사후 성과/야간 리포트/가중치 튜닝
- `src/research`: 전략 실험실(임계값/포지션 조합 검증)
- `src/notify`: 텔레그램 전송/포맷
- `src/jobs`: 주기 작업 엔트리포인트

## 설치
```bash
cd 2602_money
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
# (선택) fdr/pykrx provider까지 쓸 경우
# pip install -r requirements.providers.txt

cp .env.example .env
# .env 값 채우기 (텔레그램 토큰/chat_id, provider 등)

python scripts/init_db.py
python scripts/check_kis.py
python src/jobs/run_hourly.py
python src/jobs/run_nightly.py
```

## systemd 등록 예시
```bash
mkdir -p ~/.config/systemd/user
cp systemd/* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now 2602-money-hourly.timer
systemctl --user enable --now 2602-money-nightly.timer
systemctl --user enable --now 2602-money-watchdog.timer
systemctl --user enable --now 2602-money-chatcmd.timer
systemctl --user enable --now 2602-money-morning.timer
systemctl --user enable --now 2602-money-evening.timer
systemctl --user enable --now 2602-money-backup.timer
```

## 텔레그램 명령어 (Money_2602_bot)
- `/상태`: money/hotdeal/blog 통합 상태 대시보드
- `/뉴스`: Tech + 주요 뉴스 10건(URL 포함)
- `/최근`: 최근 스캔 후보 TOP 5
- `/도움말`

## 추가 자동화 스케줄
- `08:30` 아침 브리핑: 통합 상태 + Tech/주요 뉴스 10건
- `20:30` 저녁 통합 리포트
- `03:10` 일일 백업 (money/hotdeal/blog 핵심 파일)
- `10분 간격` watchdog: money/hotdeal/blog 상태 점검 및 자동 재기동 시도

## Provider 권장
- 운영 안정성: KIS OpenAPI 권장
- MVP/테스트: `fdr_daily`, `pykrx_daily` 가능 (스크래핑 변경 리스크 존재)

## 운영 스케줄 정책
- 실행 시간: KST 08:00~17:00, 1시간 간격
- 실행 요일: 평일
- 휴장일: KRX 캘린더(공휴일/대체휴장 포함) 자동 스킵

## 자가 업그레이드 구조
- 매 시간 결과(`candidates`)와 사후 성과(`outcomes`)를 DB에 누적합니다.
- 매 시간 종목별 가격 스냅샷(`price_snapshots`)을 저장해 1h/4h/1d 성과를 실제 미래 시점 기준으로 계산합니다.
- 야간 배치가 성과 통계와 feature-수익 상관을 계산해 가중치를 소폭 조정합니다.
- 급격한 튜닝은 막고(일일 변화폭 제한), 장기 누적 데이터 기반으로 개선되도록 설계되어 있습니다.
- 야간 배치가 성과 기반 레짐(`CONSERVATIVE`/`NEUTRAL`/`AGGRESSIVE`)을 갱신하고, 다음 장중 가상매매 진입 임계점과 포지션 크기에 반영합니다.
- 야간 `strategy_lab`이 점수 임계값/최대 보유 수 조합을 검증해 최적 조합 요약을 리포트에 표시합니다.
- 시간별 레이더 메시지에 이벤트/뉴스 리스크 점수(헤드라인 기반)가 추가됩니다.

## KIS 사용 시 준비
- KIS Developers에서 앱 등록 후 `APP_KEY`, `APP_SECRET` 발급
- `.env` 설정
  - `DATA_PROVIDER="kis"`
  - `KIS_APP_KEY`, `KIS_APP_SECRET`
  - `KIS_IS_PAPER="true"`(모의) 또는 `"false"`(실전)
- 현재 KIS provider는 공식 샘플 기준 REST 엔드포인트를 사용
  - `inquire-time-itemchartprice` (당일 분봉)
  - `inquire-daily-price` (일봉 보강)
  - `investor-trade-by-stock-daily` (수급 점수)
  - `search-stock-info` (섹터 맵 보강)

## Gemma 12B 사용(선택)
- 기본 스코어링은 규칙/통계 기반이며 LLM 토큰을 필수로 쓰지 않습니다.
- 로컬 Ollama를 켜고 아래를 설정하면 후보별 해설을 Gemma 12B로 생성합니다.
  - `ANALYST_ENABLE=true`
  - `ANALYST_BACKEND=ollama`
  - `ANALYST_MODEL=gemma3:12b`

## 가상매매(실매수/실매도 아님)
- 초기자본: `PAPER_INITIAL_CASH` (기본 1,000,000 KRW)
- 일일 체결 제한: `PAPER_MAX_TRADES_PER_DAY` (기본 10)
- 최대 보유 종목 수: `PAPER_MAX_POSITIONS` (기본 3)
- 체결 현실화: `PAPER_FEE_BPS`, `PAPER_SLIPPAGE_BPS`
- 모든 가상 주문/포지션/NAV는 DB(`paper_orders`, `paper_positions`, `paper_accounts`)에 기록됩니다.
- 보유 종목 청산 판단은 `top N`만이 아니라 해당 시점 전체 시장상태(`market_state`)를 사용합니다.
- 섹터회전(`sector_rotation`) 점수를 스코어에 반영해 업종 자금 이동을 추적합니다.

## Acceptance 기준
- `run_hourly.py`: `runs` 1개, `candidates` N개, 텔레그램 전송 1건
- `run_nightly.py`: `outcomes` 저장 + 야간 요약 전송
- Provider 변경 시 파이프라인 지속 동작

## 시작프로그램(자동 시작)
```bash
cd /home/hyeonbin/2602_money
./scripts/install_startup.sh
```
- `hourly`, `nightly`, `watchdog` 타이머를 모두 활성화합니다.
- watchdog은 10분마다 상태를 점검하고 타이머/서비스가 멈추면 자동 재기동합니다.

## 백업 복구
```bash
cd /home/hyeonbin/2602_money
./scripts/restore_backup.sh data/backups/<backup-file>.tar.gz
```
