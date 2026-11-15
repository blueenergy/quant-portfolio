# quant-portfolio

Live/paper portfolio planning service, extracted from
`stock-scoring-system`'s `portfolio/` package.

## Scope

- **Plan generation** (`portfolio/plans/generation.py`): daily monitor/rebalance
  decisions, candidate selection, plan items, risk review.
- **Scheduler** (`portfolio/scheduler.py`): activation switching, auto
  plan-generation subscriptions.
- **Paper trading** (`portfolio/paper.py`): approved-plan execution, equity
  mark-to-market.
- **Trailing stop** (`portfolio/trailing_stop.py`): stop monitoring and sell-plan
  generation.
- **Construction** (`portfolio/construction.py`): model cross-section from
  `quant_ml.model_predictions` (+ `stock_info` / PIT `index_constituents` /
  `sw_index_member`), and the legacy rule-score branch kept until Phase 5.

## Data boundary

The model path (`score_type=score_column` + `model_id`) reads only:

- `quant_ml.model_predictions`
- `quant_data.index_constituents`, `trade_calendar`, `stock_info`,
  `sw_index_member`, `sw_index_classify`, `volume_price`, `stock_adj_factor`
- `quant_trading.*`

It does **not** read `finance.stock_scores`. The rule-score branch still does
(legacy, no active live lineage).

## Layout

```
portfolio/    plan generation, scheduler, paper, trailing stop, construction, db
shared/       pure helpers copied from stock-scoring-system (no data_manager)
utils/        Mongo client
tools/        plan-generation loop, crons, paper executor, mark-to-market
start.portfolio.sh / crontab.portfolio.txt
tests/        portfolio-related unit tests
```

`shared/__init__.py` is deliberately empty: the stock-scoring version
re-exports `data_access`, which pulls in `data_manager` and the scoring stack.

## Run

```
docker build -t quant-portfolio:dev .
docker run --rm --env-file ../quant-infrastructure/apps/env/common.env \
  --env-file ../quant-infrastructure/apps/env/quant-scorer.env \
  quant-portfolio:dev
```

The service expects `TUSHARE_TOKEN` for future trading-calendar lookups
(`start.portfolio.sh` enforces it unless `PORTFOLIO_REQUIRE_TUSHARE_TOKEN=false`).

## Tests

```
pip install -e '.[dev]'
pip install -e ./data-access-lib   # git submodule in CI
pytest -q
```

## Cutover

The image is intended to replace the `quant-portfolio` compose service, which
currently runs the `quant-scorer` image. Cutover steps:

1. Publish `wukongquant/quant-portfolio:sha-*` and bump
   `QUANT_PORTFOLIO_IMAGE_TAG` in `quant-infrastructure/apps/versions.env`.
2. Change the compose service image to `quant-portfolio:${QUANT_PORTFOLIO_IMAGE_TAG}`.
3. Deploy `quant-portfolio`; rollback is reverting the image to the scorer tag.
