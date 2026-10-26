import platform
import threading
from typing import TYPE_CHECKING

from hummingbot.analytics.hooks import run
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.strategy_v2_base import StrategyV2Base

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication  # noqa: F401


class StopCommand:
    def stop(self,  # type: HummingbotApplication
             skip_order_cancellation: bool = False):
        if threading.current_thread() != threading.main_thread():
            self.ev_loop.call_soon_threadsafe(self.stop, skip_order_cancellation)
            return
        safe_ensure_future(self.stop_loop(skip_order_cancellation), loop=self.ev_loop)

    async def stop_loop(self,  # type: HummingbotApplication
                        skip_order_cancellation: bool = False):
        failure = None
        try:
            self.logger().info("stop command initiated.")
            self.notify("\nWinding down...")

            if platform.system() == "Darwin":
                import appnope
                appnope.nap()

            if self.trading_core.strategy and isinstance(self.trading_core.strategy, StrategyV2Base):
                await self.trading_core.strategy.on_stop()

            if self.trading_core._strategy_running:
                await self.trading_core.stop_strategy()

            if not skip_order_cancellation:
                await self.trading_core.cancel_outstanding_orders()

            connector_names = list(self.trading_core.connectors.keys())
            for name in connector_names:
                try:
                    self.trading_core.remove_connector(name)
                except Exception as e:
                    self.logger().error(f"Error stopping connector {name}: {e}")

            if self.trading_core._is_running:
                await self.trading_core.stop_clock()

            if self.trading_core.markets_recorder:
                self.trading_core.markets_recorder.stop()
                self.trading_core.markets_recorder = None

            self.trading_core.strategy = None
            self.trading_core.strategy_name = None
            self.trading_core.strategy_config_map = None
            self.trading_core._strategy_file_name = None
            self.trading_core._config_source = None
            self.trading_core._config_data = None

            self.notify("Hummingbot stopped.")
        except BaseException as error:
            failure = error
            raise
        finally:
            run.finish("stop_command", "user" if failure is None else None, failure)
