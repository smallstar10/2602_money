from src.core.config import Settings


def load_provider(settings: Settings):
    key = settings.data_provider.lower()
    if key == "kis":
        from src.providers.kis import KisProvider

        return KisProvider(settings)
    if key == "fdr_daily":
        from src.providers.fdr_daily import FdrDailyProvider

        return FdrDailyProvider()
    if key == "pykrx_daily":
        from src.providers.pykrx_daily import PykrxDailyProvider

        return PykrxDailyProvider()
    raise ValueError(f"unknown DATA_PROVIDER: {settings.data_provider}")
