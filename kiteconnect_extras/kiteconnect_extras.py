from __future__ import annotations
# -*- coding: utf-8 -*-
"""
    :description: KiteConnect (Batteries Included) On Steroids Version.
    :license: MIT.
    :author: Dr June Moone
    :created: On Saturday July 29, 2023 19:56:53 GMT+05:30
"""
__author__ = "Dr June Moone"
__webpage__ = "https://github.com/MooneDrJune"
__license__ = "MIT"

import os, gc, sys, copy, json, time, signal, pprint, pickle, socket, struct, asyncio, logging, urllib.parse, platform, contextlib, pandas as pd, dateutil.parser
from pathlib import Path
from functools import cache
from collections import deque
from itertools import takewhile
from json import JSONDecodeError
from types import SimpleNamespace
from pickle import HIGHEST_PROTOCOL
from functools import wraps, partial
from time import perf_counter, sleep
from datetime import datetime, timedelta
from aiohttp.resolver import AsyncResolver
from threading import Event, Thread, Timer
from multiprocessing import cpu_count, resource_tracker
from multiprocessing.shared_memory import SharedMemory, ShareableList
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import (
    Any,
    Callable,
    Dict,
    Tuple,
    List,
    Optional,
    Union,
    Literal,
    NoReturn,
    Set,
)

try:
    import six, pyotp, aiohttp, requests, kiteconnect
    import kiteconnect.exceptions as ex
    from dill import dump, load
    from six.moves.urllib.parse import urljoin
    from requests.models import PreparedRequest
    from kiteconnect import KiteConnect, KiteTicker
except (ImportError, ModuleNotFoundError):
    os.system(
        f"{sys.executable} -m pip install --upgrade dill"
        + " six pyotp requests aiohttp aiodns cchardet"
        + "  kiteconnect websocket-client websockets"
    )
    import six, pyotp, aiohttp, requests, kiteconnect
    import kiteconnect.exceptions as ex
    from dill import dump, load
    from six.moves.urllib.parse import urljoin
    from requests.models import PreparedRequest
    from kiteconnect import KiteConnect, KiteTicker

try:
    # unix / macos only
    from signal import SIGABRT, SIGINT, SIGTERM, SIGHUP

    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
except (ImportError, ModuleNotFoundError):
    from signal import SIGABRT, SIGINT, SIGTERM

    SIGNALS = (SIGABRT, SIGINT, SIGTERM)

if not platform.system().lower().startswith("win") and sys.version_info >= (
    3,
    8,
):  # noqa E501
    try:
        import uvloop
    except (ImportError, ModuleNotFoundError):
        os.system(f"{sys.executable} -m pip install uvloop")
        import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


log = logging.getLogger(__name__)


class KiteExtUtils:
    @staticmethod
    def nbytes(frame: Any) -> int:
        if isinstance(frame, (bytes, bytearray)):
            return len(frame)
        else:
            try:
                return frame.nbytes
            except AttributeError:
                return len(frame)

    @staticmethod
    def pack_frames_prelude(frames: Any) -> bytes:
        lengths = [struct.pack("Q", len(frames))] + [
            struct.pack("Q", KiteExtUtils.nbytes(frame)) for frame in frames
        ]
        return b"".join(lengths)

    @staticmethod
    def pack_frames(frames: Any) -> Tuple[int, List[bytes]]:
        prelude = [KiteExtUtils.pack_frames_prelude(frames)]
        if not isinstance(frames, list):
            frames = list(frames)
        data_ls = prelude + frames
        data_sz = sum(map(lambda b: len(b), data_ls))
        return data_sz, data_ls

    @staticmethod
    def unpack_frames(bytes_array: Any) -> List[Any]:
        frames = []
        (n_frames,) = struct.unpack("Q", bytes_array[:8])
        start = 8 + n_frames * 8
        for i in range(n_frames):
            (length,) = struct.unpack(
                "Q", bytes_array[(i + 1) * 8 : (i + 2) * 8]
            )  # noqa E501
            frame = bytes_array[start : start + length]
            frames.append(frame)
            start += length
        return frames

    @staticmethod
    def remove_shm_from_resource_tracker() -> None:
        def fix_register(name, rtype):
            if rtype == "shared_memory":
                return
            return resource_tracker._resource_tracker.register(name, rtype)  # noqa E501

        resource_tracker.register = fix_register

        def fix_unregister(name, rtype):
            if rtype == "shared_memory":
                return
            return resource_tracker._resource_tracker.unregister(
                name, rtype
            )  # noqa E501

        resource_tracker.unregister = fix_unregister

        if "shared_memory" in resource_tracker._CLEANUP_FUNCS:  # type: ignore
            del resource_tracker._CLEANUP_FUNCS["shared_memory"]  # type: ignore

    @staticmethod
    def create_shm_list(
        list_object: list, name: str
    ) -> Optional[ShareableList]:  # noqa E501
        KiteExtUtils.remove_shm_from_resource_tracker()
        try:
            return ShareableList(list_object, name=name)
        except FileExistsError:
            shared_list_object = KiteExtUtils.get_shm_list(name)
            if shared_list_object is not None:
                KiteExtUtils.close_shm_list(shared_list_object, unlink=True)
                del shared_list_object
        return ShareableList(list_object, name=name)

    @staticmethod
    def get_shm_list(name: str) -> Optional[ShareableList]:
        KiteExtUtils.remove_shm_from_resource_tracker()
        try:
            return ShareableList(name=name)
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"The Shared Memory At The Provided Namesapce: {name}"
                + "Does Not Exists, Please Check Whether The Shared Memory Exists, And If It Does, Please Pass The Correct Namesapce And Try Again."  # noqa: E501
            ) from e

    @staticmethod
    def close_shm_list(
        shared_list_object: ShareableList, unlink: bool = False
    ) -> None:  # noqa E501
        shared_list_object.shm.close()
        if unlink:
            shared_list_object.shm.unlink()

    @staticmethod
    def create_shm(
        name: str = "testshm", size: int = 10485760
    ) -> SharedMemory:  # noqa E501
        KiteExtUtils.remove_shm_from_resource_tracker()
        try:
            return SharedMemory(name=name, create=True, size=size)
        except FileExistsError:
            shm = KiteExtUtils.read_shm(name)
            if shm is not None:
                KiteExtUtils.close_shm(shm, unlink=True)
                del shm
        return SharedMemory(name=name, create=True, size=size)

    @staticmethod
    def read_shm(name: str = "testshm") -> SharedMemory:
        KiteExtUtils.remove_shm_from_resource_tracker()
        try:
            return SharedMemory(name=name)
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"The Shared Memory At The Provided Namesapce: {name}"
                + "Does Not Exists, Please Check Whether The Shared Memory Exists, And If It Does, Please Pass The Correct Namesapce And Try Again."  # noqa: E501
            ) from e

    @staticmethod
    def close_shm(shm: SharedMemory, unlink: bool = False) -> None:
        shm.close()
        if unlink:
            shm.unlink()

    @staticmethod
    def shm_send_obj(obj: Any, shm: SharedMemory) -> int:
        buffers = []
        # Pack the buffers to be written to memory
        data_sz, data_ls = KiteExtUtils.pack_frames(
            [
                pickle.dumps(
                    obj,
                    protocol=pickle.HIGHEST_PROTOCOL,
                    buffer_callback=lambda b: buffers.append(b.raw()),
                )
            ]
            + buffers
        )
        # Write to shared memory
        write_offset = 0
        for data in data_ls:
            write_end = write_offset + len(data)
            shm.buf[write_offset:write_end] = data  # type: ignore
            write_offset = write_end
        return data_sz

    @staticmethod
    def shm_recv_obj(shm: SharedMemory, size: int = 10485760) -> Any:
        data = shm.buf[:size]
        # Unpack and un-pickle the data buffers
        buffers = KiteExtUtils.unpack_frames(data)
        obj = pickle.loads(buffers[0], buffers=buffers[1:])
        # Bring the `obj` out of shared memory
        ret = copy.deepcopy(obj)
        # Clean up
        del data
        del buffers
        del obj
        return ret

    @staticmethod
    def validate_object_path(object_path: Union[Path, str]) -> Path:
        return (
            Path(
                f"{object_path.absolute()}"
                if ".pkl" in str(object_path.absolute())
                else f"{object_path.absolute()}.pkl"
            )
            if isinstance(object_path, Path)
            else Path(
                Path(
                    f"{object_path}"
                    if object_path.find(".pkl") != -1
                    else f"{object_path}.pkl"
                ).absolute()  # noqa: E501
            )  # noqa: E501
        )

    @staticmethod
    def save_object(
        objects: Any,
        object_path: Union[Path, str],
    ) -> None:
        object_path = KiteExtUtils.validate_object_path(object_path)
        if (
            isinstance(object_path, Path)
            and object_path.exists()
            and object_path.is_file()
        ):
            try:
                with open(object_path, "wb") as output_file:
                    dump(objects, output_file, protocol=HIGHEST_PROTOCOL)
                    print(
                        f"{object_path} State Saved in {object_path} File"
                    )  # noqa E501
            except Exception as err:
                print(
                    f"Failed To Save {object_path} State "
                    + f"In {object_path}.pkl File"
                    + f"\nAnd The Exception Was: {err}"
                )
                raise err
        else:
            raise ValueError(
                "The Object Path Should be either of type pathlib.Path or str"
            )  # noqa: E501

    @staticmethod
    def read_object(object_path: Union[Path, str]) -> Any:
        object_path = KiteExtUtils.validate_object_path(object_path)
        if (
            isinstance(object_path, Path)
            and object_path.exists()
            and object_path.is_file()
        ):
            try:
                with open(object_path, "rb") as input_file:
                    objects = load(input_file)
                    print(
                        f"{object_path} State Read From {object_path} File"
                    )  # noqa E501
            except Exception as err:
                print(
                    f"Failed To Read {object_path} State"
                    + f"From {object_path}.pkl File"
                    + f"\nAnd The Exception Was: {err}"
                )
                raise err
            else:
                return objects
        else:
            raise ValueError(
                "The Object Path Should be either of type pathlib.Path or str"
            )  # noqa: E501


class ProgramKilled(Exception):
    """ProgramKilled Checks the ProgramKilled exception"""

    pass  # type: ignore


class CustomTimer(Thread):
    def __init__(
        self,
        name: str,
        interval: float,
        function: Callable,
        args=None,
        kwargs=None,  # noqa E501
    ) -> None:  # noqa E501
        Thread.__init__(self=self)
        self.interval: float = interval
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = Event()
        self.start_time: float = perf_counter()
        self.next_iteration = perf_counter() + self.interval
        self.daemon = True
        self.name = name
        self.first_run = True

    def signal_handler(self, signum, frame) -> NoReturn:
        self.cancel()
        raise ProgramKilled

    def cancel(self) -> None:
        self.finished.set()
        self.join(0.5)

    def run(self) -> None:
        with contextlib.suppress(ValueError):
            for sig in SIGNALS:
                signal.signal(signalnum=sig, handler=self.signal_handler)

        while not self.finished.is_set():
            try:
                if perf_counter() >= self.next_iteration or self.first_run:
                    self.next_iteration: float = perf_counter() + self.interval
                    try:
                        self.function(*self.args, **self.kwargs)
                    except (ProgramKilled, KeyboardInterrupt, SystemExit):
                        self.finished.set()
                        break
                    if self.first_run:
                        self.first_run = False
                    if self.finished.is_set():
                        break
            except (ProgramKilled, KeyboardInterrupt, SystemExit):
                self.cancel()
                break
            sleep(0.005)
        else:
            self.finished.set()


class KiteExtTicker(KiteTicker):
    def __graceful_exit(self) -> None:
        if self.share_ticks:
            self.publish_ticks.cancel()
            self.fetch_rpcs.cancel()
            self.process_rpcs.cancel()
        if self.with_shared_ticks:
            self.subscribe_ticks.cancel()
        if hasattr(self, "ticks_shm") and self.ticks_shm is not None:
            KiteExtUtils.close_shm(self.ticks_shm, unlink=self.share_ticks)
        if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
            KiteExtUtils.close_shm(self.rpc_shm, unlink=self.share_ticks)

    def __enter__(self) -> "KiteExtTicker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.__graceful_exit()

    def __del__(self) -> None:
        self.__graceful_exit()

    def __delete__(self) -> None:
        self.__graceful_exit()

    def handle_stop_signals(self, *args, **kwargs):
        try:
            self.__graceful_exit()
        except Exception as err:
            log.error(str(err))
        else:
            raise SystemExit

    def receive_rpc(self) -> None:
        try:
            rpc_data = KiteExtUtils.shm_recv_obj(self.rpc_shm)
        except IndexError:
            pass
        else:
            self.rpc_calls_q.append(rpc_data)

    def process_rpc(self) -> None:
        if len(self.rpc_calls_q) == 0:
            return
        while len(self.rpc_calls_q) != 0:
            fn, args, kwargs = self.rpc_calls_q.popleft()
            try:
                if hasattr(self, fn):
                    fn_call_result = getattr(self, fn)(*args, **kwargs)
            except Exception as e:
                log.error(
                    f"Exception Encountered While executing remote procedure call. Exception Was: {e}"
                )

    def initialize_fetch_rpc_calls(self) -> None:
        self.rpc_calls_q = deque()
        self.rpc_shm = KiteExtUtils.create_shm(name="KiteExtTickerRpc")
        self.fetch_rpcs = CustomTimer(
            "receive_rpc_calls",
            0.05,
            self.receive_rpc,
        )
        self.process_rpcs = CustomTimer(
            "process_rpc_calls",
            0.05,
            self.process_rpc,
        )
        self.fetch_rpcs.start()
        self.process_rpcs.start()

    def publish_ticks_over_shm(self) -> None:
        self.ticks_shm = KiteExtUtils.create_shm(name="KiteExtTicker")
        self.publish_ticks = CustomTimer(
            "publish_tickers_timer",
            0.15,
            KiteExtUtils.shm_send_obj,
            (self.ticks, self.ticks_shm),
        )
        self.publish_ticks.start()

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to a list of instrument_tokens.

        - `instrument_tokens` is list of instrument instrument_tokens to subscribe
        """
        if self.with_shared_ticks:
            fn = sys._getframe().f_code.co_name
            if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
                KiteExtUtils.shm_send_obj((fn, args, kwargs), self.rpc_shm)
                self.subscribed_tokens.update(set(args[0]))
        else:
            try:
                instrument_tokens = args[0]
                self.ws.sendMessage(
                    six.b(
                        json.dumps(
                            {"a": self._message_subscribe, "v": instrument_tokens}
                        )
                    )
                )

                for token in instrument_tokens:
                    self.subscribed_tokens[token] = self.MODE_QUOTE

                return True
            except Exception as e:
                self._close(reason="Error while subscribe: {}".format(str(e)))
                raise

    def unsubscribe(self, *args, **kwargs):
        """
        Unsubscribe the given list of instrument_tokens.

        - `instrument_tokens` is list of instrument_tokens to unsubscribe.
        """
        if self.with_shared_ticks:
            fn = sys._getframe().f_code.co_name
            if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
                KiteExtUtils.shm_send_obj((fn, args, kwargs), self.rpc_shm)
                for token in args[0]:
                    try:
                        self.subscribed_tokens.remove(token)
                    except KeyError:
                        pass
        else:
            try:
                instrument_tokens = args[0]
                self.ws.sendMessage(
                    six.b(
                        json.dumps(
                            {"a": self._message_unsubscribe, "v": instrument_tokens}
                        )
                    )
                )

                for token in instrument_tokens:
                    try:
                        del self.subscribed_tokens[token]
                    except KeyError:
                        pass

                return True
            except Exception as e:
                self._close(reason="Error while unsubscribe: {}".format(str(e)))
                raise

    def set_mode(self, *args, **kwargs):
        """
        Set streaming mode for the given list of tokens.

        - `mode` is the mode to set. It can be one of the following class constants:
            MODE_LTP, MODE_QUOTE, or MODE_FULL.
        - `instrument_tokens` is list of instrument tokens on which the mode should be applied
        """
        if self.with_shared_ticks:
            fn = sys._getframe().f_code.co_name
            if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
                KiteExtUtils.shm_send_obj((fn, args, kwargs), self.rpc_shm)
        else:
            try:
                mode, instrument_tokens = args[:2]
                self.ws.sendMessage(
                    six.b(
                        json.dumps(
                            {"a": self._message_setmode, "v": [mode, instrument_tokens]}
                        )
                    )
                )

                # Update modes
                for token in instrument_tokens:
                    self.subscribed_tokens[token] = mode

                return True
            except Exception as e:
                self._close(reason="Error while setting mode: {}".format(str(e)))
                raise
        
    def process_subscribed_ticks_over_shm(self) -> None:
        if not hasattr(self, "ticks_shm"):
            try:
                self.ticks_shm = KiteExtUtils.read_shm(name="KiteExtTicker")
            except FileNotFoundError:
                pass
        else:
            try:
                latest_ticks = KiteExtUtils.shm_recv_obj(self.ticks_shm)
            except IndexError:
                pass
            else:
                if isinstance(latest_ticks, dict):
                    self.ticks.update(
                        {
                            k: v
                            for k, v in latest_ticks.items()
                            if k in self.subscribed_tokens or k == "order_update"
                        }
                    )
                del latest_ticks
        if not hasattr(self, "rpc_shm"):
            try:
                self.rpc_shm = KiteExtUtils.read_shm(name="KiteExtTickerRpc")
            except FileNotFoundError:
                pass

    def subscribe_ticks_over_shm(self) -> None:
        self.subscribe_ticks = CustomTimer(
            "subscribe_tickers_timer",
            0.15,
            self.process_subscribed_ticks_over_shm,
        )
        self.subscribe_ticks.start()

    def __init__(
        self,
        user_id: str = "",
        enctoken: str = "",
        root_uri: str = "wss://ws.zerodha.com",
        api_key: str = "kitefront",
        user_agent: str = "kite3-web",
        version: str = "3.0.9",
        share_ticks: bool = False,
        with_shared_ticks_only: bool = False,
        **kwargs,
    ) -> None:
        self.share_ticks = share_ticks
        self.with_shared_ticks = with_shared_ticks_only
        if not self.with_shared_ticks:
            if user_id == "" or enctoken == "":
                raise ValueError(
                    "`user_id` and `enctoken` value can not be empty, Please Try again with correct value of both `user_id` and `enctoken`."
                )
            access_token = enctoken
            KiteTicker.__init__(self, api_key, access_token, **kwargs)
            self.socket_url = (
                "{root}?api_key={api_key}"
                "&user_id={user_id}"
                "&enctoken={enctoken}"
                "&user-agent={user_agent}"
                "&version={version}".format(
                    root=root_uri,
                    api_key=api_key,
                    user_id=user_id,
                    enctoken=urllib.parse.quote(enctoken),
                    user_agent=user_agent,
                    version=version,
                )
            )
            self.default_divisor = 100.0
            self.cds_divisor = 10000000.0
            self.bcd_divisor = 10000.0
        if self.share_ticks or self.with_shared_ticks:
            self.ticks: Dict[str, Any] = {"order_update": {}}
        if self.share_ticks:
            self.publish_ticks_over_shm()
            self.initialize_fetch_rpc_calls()
        if self.with_shared_ticks:
            self.subscribe_ticks_over_shm()

    def _parse_text_message(self, payload):
        """Parse text message."""
        # Decode unicode data
        if not six.PY2 and type(payload) == bytes:
            payload = payload.decode("utf-8")

        try:
            data = json.loads(payload)
        except ValueError:
            return

        # Order update callback
        if self.on_order_update and data.get("type") == "order":
            if self.share_ticks:
                self.ticks["order_update"] = data
            self.on_order_update(self, data)

        # Custom error with websocket error code 0
        if data.get("type") == "error":
            self._on_error(self, 0, data)

    def _decode_and_process_binary_packet(
        self, packet: Any
    ) -> Optional[Dict[str, Any]]:
        tick: Optional[Dict[str, Any]] = None
        instrument_token = self._unpack_int(packet, 0, 4)
        # Retrive segment constant from instrument_token
        segment = instrument_token & 0xFF
        # Add price divisor based on segment
        if segment == self.EXCHANGE_MAP["cds"]:
            divisor = self.cds_divisor
        elif segment == self.EXCHANGE_MAP["bcd"]:
            divisor = self.bcd_divisor
        else:
            divisor = self.default_divisor
        # All indices are not tradable
        tradable = segment != self.EXCHANGE_MAP["indices"]

        # LTP packets
        if len(packet) == 8:
            tick = {
                "tradable": tradable,
                "mode": self.MODE_LTP,
                "instrument_token": instrument_token,
                "last_price": self._unpack_int(packet, 4, 8) / divisor,
            }
        # Indices quote and full mode
        elif len(packet) == 28 or len(packet) == 32:
            mode = self.MODE_QUOTE if len(packet) == 28 else self.MODE_FULL
            tick = {
                "tradable": tradable,
                "mode": mode,
                "instrument_token": instrument_token,
                "last_price": self._unpack_int(packet, 4, 8) / divisor,
                "ohlc": {
                    "high": self._unpack_int(packet, 8, 12) / divisor,
                    "low": self._unpack_int(packet, 12, 16) / divisor,
                    "open": self._unpack_int(packet, 16, 20) / divisor,
                    "close": self._unpack_int(packet, 20, 24) / divisor,
                },
            }
            # Compute the change price using close price and last price
            tick["change"] = 0
            if tick["ohlc"]["close"] != 0:
                tick["change"] = (
                    (tick["last_price"] - tick["ohlc"]["close"])
                    * 100
                    / tick["ohlc"]["close"]
                )
            # Full mode with timestamp
            if len(packet) == 32:
                try:
                    timestamp = datetime.fromtimestamp(self._unpack_int(packet, 28, 32))
                except Exception:
                    timestamp = None
                tick["exchange_timestamp"] = timestamp
        # Quote and full mode
        elif len(packet) == 44 or len(packet) == 184:
            mode = self.MODE_QUOTE if len(packet) == 44 else self.MODE_FULL
            tick = {
                "tradable": tradable,
                "mode": mode,
                "instrument_token": instrument_token,
                "last_price": self._unpack_int(packet, 4, 8) / divisor,
                "last_traded_quantity": self._unpack_int(packet, 8, 12),
                "average_traded_price": self._unpack_int(packet, 12, 16) / divisor,
                "volume_traded": self._unpack_int(packet, 16, 20),
                "total_buy_quantity": self._unpack_int(packet, 20, 24),
                "total_sell_quantity": self._unpack_int(packet, 24, 28),
                "ohlc": {
                    "open": self._unpack_int(packet, 28, 32) / divisor,
                    "high": self._unpack_int(packet, 32, 36) / divisor,
                    "low": self._unpack_int(packet, 36, 40) / divisor,
                    "close": self._unpack_int(packet, 40, 44) / divisor,
                },
            }

            # Compute the change price using close price and last price
            tick["change"] = 0
            if tick["ohlc"]["close"] != 0:
                tick["change"] = (
                    (tick["last_price"] - tick["ohlc"]["close"])
                    * 100
                    / tick["ohlc"]["close"]
                )

            # Parse full mode
            if len(packet) == 184:
                try:
                    last_trade_time = datetime.fromtimestamp(
                        self._unpack_int(packet, 44, 48)
                    )
                except Exception:
                    last_trade_time = None

                try:
                    timestamp = datetime.fromtimestamp(self._unpack_int(packet, 60, 64))
                except Exception:
                    timestamp = None

                tick["last_trade_time"] = last_trade_time
                tick["oi"] = self._unpack_int(packet, 48, 52)
                tick["oi_day_high"] = self._unpack_int(packet, 52, 56)
                tick["oi_day_low"] = self._unpack_int(packet, 56, 60)
                tick["exchange_timestamp"] = timestamp

                # Market depth entries.
                depth = {"buy": [], "sell": []}
                # Compile the market depth lists.
                [
                    depth["sell" if i >= 5 else "buy"].append(
                        {
                            "quantity": self._unpack_int(packet, p, p + 4),
                            "price": self._unpack_int(packet, p + 4, p + 8) / divisor,
                            "orders": self._unpack_int(
                                packet, p + 8, p + 10, byte_format="H"
                            ),
                        }
                    )
                    for i, p in enumerate(range(64, len(packet), 12))
                ]
                tick["depth"] = depth
        else:
            pass
        if self.share_ticks:
            self.ticks[instrument_token] = tick
        return tick

    def _parse_binary(self, bin):
        """Parse binary data to a (list of) ticks structure."""
        return [
            self._decode_and_process_binary_packet(packet)
            for packet in self._split_packets(bin)  # noqa E501
        ]


class KiteExt(KiteConnect):
    @staticmethod
    async def on_request_start(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestStartParams,
    ) -> None:
        trace_config_ctx.start = asyncio.get_event_loop().time()
        log.debug(
            " Request Hook: Initiated HTTP.%s Request To Url: %s | With Header: %s |",
            params.method,
            params.url,
            json.dumps(dict(params.headers)),
        )  # noqa E501
        print("\n")

    @staticmethod
    async def on_request_end(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestEndParams,
    ) -> None:
        elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start
        elapsed_msg = f"{elapsed:.3f} Seconds"
        text = await params.response.text()
        if len(text) > 500:
            text = f"{text[:500]}...Truncated to 500 Characters."
        log.debug(
            " Response Hook: The HTTP.%s Request To Url: %s | Completed In %s | Response Status: %s %s | Response Header: %s | Response Content: %s |",
            params.method,
            params.url,
            elapsed_msg,
            params.response.status,
            params.response.reason,
            json.dumps(dict(params.headers)),
            text,
        )
        print("\n")

    @staticmethod
    async def on_request_exception(
        session: aiohttp.ClientSession,
        trace_config_ctx: SimpleNamespace,
        params: aiohttp.TraceRequestExceptionParams,
    ) -> None:
        log.debug(
            " Request Exception Hook: The HTTP.%s Request To Url: %s | Request Header: %s | Failed With Exception: %s |",
            params.method,
            params.url,
            json.dumps(dict(params.headers)),
            str(params.exception),
        )
        print("\n")

    @staticmethod
    async def generate_async_client_session(
        base_url: str,
        connector: aiohttp.TCPConnector,
        headers: Dict[str, Union[str, Any]],
        timeout: aiohttp.ClientTimeout,
        raise_for_status: bool,
        trust_env: bool,
        trace_configs: Optional[List[aiohttp.TraceConfig]] = None,
        cookie_jar: Optional[aiohttp.CookieJar] = None,
    ) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            base_url=base_url,
            connector=connector,
            headers=headers,
            timeout=timeout,
            cookie_jar=cookie_jar,
            raise_for_status=raise_for_status,
            trust_env=trust_env,
            trace_configs=trace_configs,
        )

    @staticmethod
    def totp(qrcode: str) -> str:
        return pyotp.TOTP(qrcode).now().zfill(6)

    @staticmethod
    def start_background_loop(
        loop: asyncio.AbstractEventLoop,
    ) -> Optional[NoReturn]:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        except (KeyboardInterrupt, SystemExit, ProgramKilled):
            loop.run_until_complete(loop.shutdown_asyncgens())
            if loop.is_running():
                loop.stop()
            if not loop.is_closed():
                loop.close()

    def graceful_exit(self) -> None:
        if self.with_shared_tick_data:
            self.subscribe_ticks.cancel()
        if hasattr(self, "ticks_shm") and self.ticks_shm is not None:
            KiteExtUtils.close_shm(self.ticks_shm)
        if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
            KiteExtUtils.close_shm(self.rpc_shm)
        if self.share_historical_data:
            self.publish_historical.cancel()
        if self.with_shared_historical_data:
            self.subscribe_historical.cancel()
        if hasattr(self, "historical_shm") and self.historical_shm is not None:
            KiteExtUtils.close_shm(
                self.historical_shm,
                unlink=self.share_historical_data,
            )
        with contextlib.suppress(RuntimeError, RuntimeWarning):
            asyncio.run_coroutine_threadsafe(self.__reqsession.close(), self.loop)
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0.25), self.loop)
            asyncio.run_coroutine_threadsafe(self.__web_reqsession.close(), self.loop)
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0.25), self.loop)
            asyncio.run_coroutine_threadsafe(self.loop.shutdown_asyncgens(), self.loop)
            if self.loop.is_running():
                self.loop.stop()
            if not self.loop.is_closed():
                self.loop.close()

    def __aenter__(self) -> "KiteExt":
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.graceful_exit()

    def __enter__(self) -> "KiteExt":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.graceful_exit()

    def __del__(self) -> None:
        self.graceful_exit()

    def __delete__(self) -> None:
        self.graceful_exit()

    def subscribe(self, *args, **kwargs):
        fn = sys._getframe().f_code.co_name
        if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
            KiteExtUtils.shm_send_obj((fn, args, kwargs), self.rpc_shm)
            self.subscribed_tokens.update(set(args[0]))

    def unsubscribe(self, *args, **kwargs):
        fn = sys._getframe().f_code.co_name
        if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
            KiteExtUtils.shm_send_obj((fn, args, kwargs), self.rpc_shm)
            for token in args[0]:
                try:
                    self.subscribed_tokens.remove(token)
                except KeyError:
                    pass

    def set_mode(self, *args, **kwargs):
        fn = sys._getframe().f_code.co_name
        if hasattr(self, "rpc_shm") and self.rpc_shm is not None:
            KiteExtUtils.shm_send_obj((fn, args, kwargs), self.rpc_shm)

    def process_subscribed_ticks_over_shm(self) -> None:
        if not hasattr(self, "ticks_shm"):
            try:
                self.ticks_shm = KiteExtUtils.read_shm(name="KiteExtTicker")
            except FileNotFoundError:
                pass
        else:
            try:
                latest_ticks = KiteExtUtils.shm_recv_obj(self.ticks_shm)
            except IndexError:
                pass
            else:
                if isinstance(latest_ticks, dict):
                    self.ticks.update(
                        {
                            k: v
                            for k, v in latest_ticks.items()
                            if k in self.subscribed_tokens or k == "order_update"
                        }
                    )
                del latest_ticks
        if not hasattr(self, "rpc_shm"):
            try:
                self.rpc_shm = KiteExtUtils.read_shm(name="KiteExtTickerRpc")
            except FileNotFoundError:
                pass

    def subscribe_ticks_over_shm(self) -> None:
        self.subscribe_ticks = CustomTimer(
            "subscribe_tickers_timer",
            0.15,
            self.process_subscribed_ticks_over_shm,
        )
        self.subscribe_ticks.start()

    def __share_historical_data(
        self, list_of_kwargs: List[Dict[str, Any]], return_df: bool = True
    ) -> None:
        historical_datas = self.multiple_historical_data(
            list_of_kwargs, return_df=return_df, return_token_wise_df=True
        )
        if (
            historical_datas is not None
            and isinstance(historical_datas, dict)
            and self.shared_historical_datas is not None
        ):
            self.shared_historical_datas.update(historical_datas)
            try:
                KiteExtUtils.shm_send_obj(
                    self.shared_historical_datas, self.historical_shm
                )
            except FileNotFoundError:
                pass

    def _share_historical_data(
        self,
        instrument_tokens: List[int],
        refresh_interval: float,
        interval: str = "minute",
        continuous: bool = False,
        oi: bool = False,
        from_date_time: Optional[Union[str, datetime]] = None,
        to_date_time: Optional[Union[str, datetime]] = None,
        time_delta: Optional[timedelta] = None,
        no_of_days: Optional[int] = None,
        show_as_df: bool = True,
    ) -> None:
        if len(self.historical_instrument_tokens) != 0:
            raise ValueError("List Of Instrument Tokens Found To Be A Non Empty Set.")
        self.historical_instrument_tokens.update(set(instrument_tokens))
        self.shared_historical_datas = dict.fromkeys(self.historical_instrument_tokens)
        if from_date_time is None and time_delta is None and no_of_days is None:
            from_date_time = datetime.today().replace(hour=9, minute=15, second=0)
        if to_date_time is None and time_delta is None and no_of_days is None:
            to_date_time = datetime.today().replace(hour=15, minute=30, second=0)
        if time_delta is not None and to_date_time is None and from_date_time is None:
            to_date_time = datetime.today().replace(hour=15, minute=30, second=0)
            from_date_time = to_date_time - time_delta
        if no_of_days is not None and to_date_time is None and from_date_time is None:
            to_date_time = datetime.today().replace(hour=15, minute=30, second=0)
            from_date_time = to_date_time - timedelta(
                days=no_of_days, hours=6, minutes=15
            )
        print(from_date_time, to_date_time)
        list_of_kwargs = [
            {
                "instrument_token": inst,
                "from_date": from_date_time,
                "to_date": to_date_time,
                "interval": interval,
                "continuous": continuous,
                "oi": oi,
            }
            for inst in instrument_tokens
        ]
        self.historical_shm = KiteExtUtils.create_shm(name="KiteExtHistorical")
        self.publish_historical = CustomTimer(
            "historical_data_timer",
            refresh_interval - 0.05,
            self.__share_historical_data,
            (list_of_kwargs,),
            {"return_df": show_as_df},
        )
        self.publish_historical.start()

    def __refresh_shared_historical_data(self) -> None:
        if not hasattr(self, "historical_shm"):
            try:
                self.historical_shm = KiteExtUtils.read_shm(name="KiteExtHistorical")
            except FileNotFoundError:
                pass
        try:
            latest_historical = KiteExtUtils.shm_recv_obj(self.historical_shm)
        except IndexError:
            pass
        else:
            if (
                latest_historical is not None
                and isinstance(latest_historical, dict)
                and self.shared_historical_datas is not None
            ):
                self.shared_historical_datas.update(
                    {
                        k: v
                        for k, v in latest_historical.items()
                        if k in self.historical_instrument_tokens
                    }
                )
            del latest_historical

    def refresh_shared_historical_data(
        self,
        instrument_tokens: Optional[List[int]] = None,
    ) -> None:
        if instrument_tokens is not None:
            self.historical_instrument_tokens.update(set(instrument_tokens))
        if len(self.historical_instrument_tokens) == 0:
            raise ValueError(
                "List Of Instrument Tokens, Needed To Synchronize Shared Historical Data Between Sperate Instances / Processes."
            )
        self.shared_historical_datas = dict.fromkeys(self.historical_instrument_tokens)
        self.subscribe_historical = CustomTimer(
            "subscribe_tickers_timer",
            0.5,
            self.__refresh_shared_historical_data,
        )
        self.subscribe_historical.start()

    def __init__(
        self,
        api_key: str = "kitefront",
        user_id: Optional[str] = None,
        with_shared_tick_data: bool = False,
        share_historical_data: bool = False,
        with_shared_historical_data: bool = False,
        with_shared_historical_data_only: bool = False,
        subscribe_tick_inst_tokens: Optional[List[int]] = None,
        subscribe_historical_inst_tokens: Optional[List[int]] = None,
        *args,
        **kwargs,
    ):
        self.__get_delete_method = {"GET", "DELETE"}
        self.__post_put_method = {"POST", "PUT"}
        self.ticks: Dict[str, Any] = {"order_update": {}}
        self.with_shared_tick_data = with_shared_tick_data
        self.subscribed_tokens: Set[int] = (
            set(subscribe_tick_inst_tokens)
            if subscribe_tick_inst_tokens is not None and with_shared_tick_data
            else set()
        )
        self.share_historical_data = share_historical_data
        self.with_shared_historical_data = with_shared_historical_data
        self.with_shared_historical_data_only = with_shared_historical_data_only
        self.historical_instrument_tokens: Set[int] = (
            set(subscribe_historical_inst_tokens)
            if subscribe_historical_inst_tokens is not None
            and with_shared_historical_data
            else set()
        )
        self.shared_historical_datas: Optional[
            Dict[int, Union[Dict[str, Any], pd.DataFrame, None]]
        ] = None
        if not self.with_shared_historical_data_only:
            KiteConnect.__init__(self, api_key=api_key, *args, **kwargs)
            if user_id is not None:
                self.user_id = user_id
            self._routes.update(
                {
                    "api.login": "/api/login",
                    "api.twofa": "/api/twofa",
                    "api.misdata": "/margins/equity",
                    "web.historical": "/oms/instruments/historical/{instrument_token}/{interval}",
                }
            )
            self._kite_web_uri = self._default_login_uri[
                : self._default_login_uri.find("/", 8)
            ]
            self.__initialize_loop()
            self.__user_agent = self._user_agent()
            self.__web_user_agent = self._browser_user_agent()
            self.__headers = {
                "X-Kite-Version": self.kite_header_version,
                "User-Agent": self.__user_agent,
            }
            self.__web_headers = {
                "X-Kite-Version": self.kite_header_version + ".0.9",
                "User-Agent": self.__web_user_agent,
            }
            self._initialize_session_params()
        if self.with_shared_tick_data:
            self.subscribe_ticks_over_shm()
        if self.with_shared_historical_data:
            self.refresh_shared_historical_data()

    def handle_stop_signals(self, *args, **kwargs):
        try:
            self.graceful_exit()
        except Exception as err:
            log.error(str(err))
        else:
            raise SystemExit

    def __initialize_loop(self) -> None:
        global resolver
        if platform.system().lower().find("win") == -1:
            self.loop = uvloop.new_event_loop()
        else:
            self.loop = asyncio.new_event_loop()
        if platform.system().lower().startswith("win"):
            with contextlib.suppress(ValueError):
                for sig in (SIGABRT, SIGINT, SIGTERM):
                    signal.signal(sig, self.handle_stop_signals)
        else:
            with contextlib.suppress(ValueError):
                for sig in (SIGABRT, SIGINT, SIGTERM, SIGHUP):
                    self.loop.add_signal_handler(
                        sig, self.handle_stop_signals
                    )  # noqa E501
        self.event_thread = Thread(
            target=self.start_background_loop,
            args=(self.loop,),
            name=f"{self.__class__.__name__}_event_thread",
            daemon=True,
        )
        self.event_thread.start()
        log.debug("Asyncio Event Loop has been initialized.")

    async def __initialize_session_params(self) -> None:
        self.__timeout = aiohttp.ClientTimeout(
            total=float(self._default_timeout)
        )  # noqa E501
        self.__resolver = AsyncResolver(nameservers=["1.1.1.1", "1.0.0.1"])
        self.__connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=0,
            ttl_dns_cache=22500,
            use_dns_cache=True,
            resolver=self.__resolver,
            family=socket.AF_INET,
        )
        cookie_jar = aiohttp.CookieJar()
        self.__trace_config = aiohttp.TraceConfig()
        self.__trace_config.on_request_start.append(self.on_request_start)
        self.__trace_config.on_request_end.append(self.on_request_end)
        self.__trace_config.on_request_exception.append(self.on_request_exception)
        self.__reqsession = await self.generate_async_client_session(
            base_url=self.root,
            connector=self.__connector,
            headers=self.__headers,
            timeout=self.__timeout,
            raise_for_status=True,
            trust_env=True,
            trace_configs=[self.__trace_config] if self.debug else None,
            cookie_jar=cookie_jar,
        )
        self.__web_reqsession = await self.generate_async_client_session(
            base_url=self._default_login_uri[:-14],
            connector=self.__connector,
            headers=self.__web_headers,
            timeout=self.__timeout,
            raise_for_status=True,
            trust_env=True,
            trace_configs=[self.__trace_config] if self.debug else None,
        )
        await self.__reqsession.get("/")
        await self.__web_reqsession.get("/")

    async def __fetch_browser_user_agent(self) -> Optional[str]:
        async with aiohttp.ClientSession(
            base_url="https://techfanetechnologies.github.io",
            timeout=aiohttp.ClientTimeout(total=60),  # noqa E501
            raise_for_status=True,
        ) as session:
            for retry in range(3):
                try:
                    async with session.get(
                        "/latest-user-agent/user_agents.json"
                    ) as response:  # noqa E501
                        _user_agent = await response.json()
                except aiohttp.ClientError as err:
                    log.error(f"User Agent Fetch Failed with exception {err}")
                    await asyncio.sleep(1.0)
                    log.error(f"Retrying..., Retry No.{retry+1}")
                else:
                    return _user_agent[0]

    def _initialize_session_params(self) -> None:
        future = asyncio.run_coroutine_threadsafe(
            self.__initialize_session_params(),
            self.loop,
        )
        try:
            result = future.result(float(self._default_timeout))
        except TimeoutError:
            error_message = f"The Initialization of Async Client Session Took Longer Than The Default Timeout To Wait For The Response, i.e. {float(self._default_timeout):.2f} Seconds, Cancelling The Task..."
            log.error(error_message)
            future.cancel()
        except Exception as exc:
            log.exception(
                f"The Initialization of Async Client Session Ended Up With An Exception: {exc!r} {future.exception(1.0)}"
            )

    def login_using_enctoken(
        self, userid: str, enctoken: str, public_token: Optional[str] = None
    ) -> None:
        self.user_id = userid
        self.enctoken = enctoken
        self.public_token = public_token
        self.set_headers(enctoken, userid=userid)

    async def __login_with_credentials(
        self,
        userid: str,
        password: str,
        secret: str,
    ) -> Optional[Tuple[Optional[str], Optional[str], Optional[str]]]:
        try:
            async with self.__reqsession.post(
                url=self._routes["api.login"],
                data={"user_id": self.user_id, "password": self.password},
            ) as resp:
                resp_text = await resp.text()
                if not self.debug:
                    print(
                        f"The HTTP.{resp.method} Request To Url: {resp.url} | Response Status: {resp.status} {resp.reason} | Response Header: {json.dumps(dict(resp.headers))} | Response Content: {resp_text} |"
                    )
        except aiohttp.ClientError as err:
            print(f"The Login Request Failed With An Exception: {err}")
        else:
            try:
                json_body = await resp.json()
            except (JSONDecodeError, ValueError):
                print(f"The Login Request Has Got A Non-Json Response: { resp_text}")
            else:
                try:
                    async with self.__reqsession.post(
                        url=self._routes["api.twofa"],
                        data={
                            "user_id": json_body["data"]["user_id"],
                            "request_id": json_body["data"]["request_id"],
                            "twofa_value": self.totp(self.twofa_secret),
                        },
                    ) as resp:
                        resp_text = await resp.text()
                        if not self.debug:
                            print(
                                f"The HTTP.{resp.method} Request To Url: {resp.url} | Response Status: {resp.status} {resp.reason} | Response Header: {resp.headers} | Response Content: {resp_text} |"
                            )
                except aiohttp.ClientError as err:
                    print(
                        f"The TwoFa Authentication Request Failed With An Exception: {err}"
                    )
                else:
                    self.reqsession_set_cookies_value = [
                        str(resp.cookies[item])
                        for item in (
                            "user_id",
                            "enctoken",
                            "public_token",
                            "kf_session",
                            "_cfuvid",
                        )
                        if resp.cookies.get(item) is not None
                    ]
                    cookies = self.__reqsession.cookie_jar.filter_cookies(
                        self._default_root_uri
                    )
                    for key, cookie in cookies.items():
                        if cookie.key == "enctoken":
                            self.enctoken = cookie.value
                        if cookie.key == "user_id":
                            self.user_id = cookie.value
                        if cookie.key == "kf_session":
                            self.kf_session = cookie.value
                        if cookie.key == "_cfuvid":
                            self.cfuvid = cookie.value
                    if resp.cookies.get("public_token") is not None:
                        self.public_token = (
                            str(resp.cookies["public_token"])
                            .split("public_token=")[1]
                            .split("; ")[0]
                        )
                    if (
                        self.enctoken is not None
                        and self.public_token is not None
                        and self.user_id is not None
                    ):
                        self.set_headers(
                            self.enctoken,
                            public_token=self.public_token,
                            userid=self.user_id,
                        )
                    pprint.pprint(
                        {
                            "user_id": self.user_id,
                            "enctoken": self.enctoken,
                            "public_token": self.public_token,
                            "kf_session": self.kf_session,
                            "_cfuvid": self.cfuvid,
                        }
                    )

    def login_with_credentials(
        self,
        userid: str,
        password: str,
        secret: str,
    ) -> None:
        self.user_id = userid
        self.password = password
        if len(secret) != 32:
            raise ValueError("Incorrect TOTP Base32 Secret Key")
        self.twofa_secret = secret
        self.remove_auth_headers()
        future = asyncio.run_coroutine_threadsafe(
            self.__login_with_credentials(userid, password, secret),
            self.loop,
        )
        try:
            future.result(float(self._default_timeout * 2))
        except TimeoutError:
            error_message = f"The Login With Credentials Request's Took Longer Than The Default Timeout To Wait For The Response, i.e. {float(self._default_timeout * 2):.2f} Seconds, Cancelling The Task..."
            log.error(error_message)
            future.cancel()
        except Exception as exc:
            log.exception(
                f"The Login With Credentials Requests Ended Up With An Exception: {exc!r} {future.exception(1.0)}"
            )

    def _browser_user_agent(self) -> Optional[str]:
        future = asyncio.run_coroutine_threadsafe(
            self.__fetch_browser_user_agent(),
            self.loop,
        )
        try:
            result = future.result(float(self._default_timeout))
        except asyncio.TimeoutError:
            error_message = f"The Request Took Longer Than The Default Timeout To Wait For The Response, i.e. {float(self._default_timeout):.2f} Seconds, Cancelling The Task..."
            log.error(error_message)
            future.cancel()
        except Exception as exc:
            log.exception(f"The Request Ended Up With An Exception: {exc!r}")
        else:
            return result

    def set_headers(
        self,
        enctoken: str,
        public_token: Optional[str] = None,
        userid: Optional[str] = None,
    ) -> None:
        self.enctoken = enctoken
        self.public_token = public_token
        if userid is not None:
            self.user_id = userid
        else:
            raise ValueError(
                f"userid cannot be `None`, either login with credentials first or set userid here"
            )
        auth_header = {
            "Authorization": "enctoken {}".format(enctoken),
        }
        self.__headers.update(auth_header)
        self.__web_headers.update(auth_header)
        self.__reqsession.headers.update(auth_header)
        self.__web_reqsession.headers.update(auth_header)

    def remove_auth_headers(self) -> None:
        if "Authorization" in self.__headers:
            self.__headers.pop("Authorization", None)
        if "Authorization" in self.__reqsession.headers:
            self.__reqsession.headers.pop("Authorization", None)

    async def __feth_historical_data_from_web(
        self,
        url: str,
        params: Dict[str, Union[List[Any], Any]],
        limit: asyncio.Semaphore = asyncio.Semaphore(2),
        return_df: bool = False,
    ) -> Optional[
        Union[Dict[str, List[List[Union[str, int, float, datetime]]]], pd.DataFrame]
    ]:
        for no_of_retries in range(0, 10):
            if no_of_retries > 0:
                msg = f"Retry No: {no_of_retries} for Url: {url}"
                log.debug(msg)
                del msg
            async with limit:
                try:
                    response = await self.__web_reqsession.get(
                        url=url,
                        params=params,
                        verify_ssl=not self.disable_ssl,
                        allow_redirects=True,
                    )
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    sleep_for = no_of_retries * 1.137
                    msg = (
                        f"Fetch of Url: {url} with Params: {params} Met With "
                        f"An Exception: {e} | Going to sleep for"
                        f"{sleep_for} Seconds, And Retry Again..."
                    )
                    log.debug(msg)
                    del msg
                    await asyncio.sleep(sleep_for)
                    continue
                else:
                    if response.headers.get("content-type") == "application/json":
                        try:
                            json_body = await response.json()
                        except (JSONDecodeError, ValueError):
                            content = await response.text()
                            raise ex.DataException(
                                "Could not parse the JSON response received from the server: {content}".format(
                                    content=content
                                )
                            )
                        else:
                            if (
                                isinstance(json_body, dict)
                                and json_body.get("error_type") is not None
                                and json_body.get("status") == "error"
                            ):
                                # Call session hook if its registered and
                                # TokenException is raised.
                                if (
                                    self.session_expiry_hook
                                    and response.status == 403
                                    and json_body["error_type"] == "TokenException"
                                ):
                                    self.session_expiry_hook()
                                # native Kite errors
                                exp = getattr(
                                    ex, json_body.get("error_type"), ex.GeneralException
                                )
                                raise exp(json_body["message"], code=response.status)
                            return self._format_historical(
                                json_body["data"], return_df=return_df
                            )
                    else:
                        content = await response.text()
                        raise ex.DataException(
                            "Unknown Content-Type ({content_type}) with response: ({content})".format(
                                content_type=response.headers.get("content-type"),
                                content=content,
                            )
                        )

    async def __feth_Multiple_historical_data_from_web(
        self,
        url_args_params,
        limit,
        return_df: bool = False,
        return_token_wise_df: bool = False,
    ) -> Union[
        Dict[int, Optional[Union[Dict[str, Any], pd.DataFrame]]],
        List[Union[Dict[str, Any], pd.DataFrame, None]],
    ]:
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = {
                    inst_token: tg.create_task(
                        self.__feth_historical_data_from_web(
                            self._routes["web.historical"].format(**url_args),
                            params,
                            limit=limit,
                            return_df=return_df,
                        )
                    )
                    for inst_token, (url_args, params) in url_args_params
                }
            if return_token_wise_df:
                return {inst_token: task.result() for inst_token, task in tasks.items()}
            else:
                return [task.result() for task in tasks.values()]
        except AttributeError:
            inst_tokens = [
                inst_token for inst_token, (url_args, params) in url_args_params
            ]
            tasks = await asyncio.gather(
                *[
                    asyncio.ensure_future(
                        self.__feth_historical_data_from_web(
                            self._routes["web.historical"].format(**url_args),
                            params,
                            limit=limit,
                            return_df=return_df,
                        )
                    )
                    for inst_token, (url_args, params) in url_args_params
                ]
            )
            if return_token_wise_df:
                return {
                    inst_token: task.result()
                    for inst_token, task in zip(inst_tokens, tasks)
                }
            else:
                return [task.result() for task in tasks]

    def historical_data(
        self,
        instrument_token: Union[int, str],
        from_date: Union[datetime, str],
        to_date: Union[datetime, str],
        interval: str,
        continuous: bool = False,
        oi: bool = False,
        web_api: bool = False,
        return_df: bool = False,
        return_kwargs: bool = False,
    ) -> Union[
        Optional[
            Union[
                Dict[str, List[List[Union[str, int, float, datetime]]]], pd.DataFrame
            ],
        ],
        Tuple[Dict[str, Union[str, int]], Dict[str, Union[str, int]]],
    ]:
        """
        Retrieve historical data (candles) for an instrument.

        Although the actual response JSON from the API does not have field
        names such has 'open', 'high' etc., this function call structures
        the data into an array of objects with field names. For example:

        - `instrument_token` is the instrument identifier (retrieved from the instruments()) call.
        - `from_date` is the From date (datetime object or string in format of yyyy-mm-dd HH:MM:SS.
        - `to_date` is the To date (datetime object or string in format of yyyy-mm-dd HH:MM:SS).
        - `interval` is the candle interval (minute, day, 5 minute etc.).
        - `continuous` is a boolean flag to get continuous data for futures and options instruments.
        - `oi` is a boolean flag to get open interest.
        """
        date_string_format = "%Y-%m-%d %H:%M:%S"
        from_date_string = (
            from_date.strftime(date_string_format)
            if isinstance(from_date, datetime)
            else from_date
        )
        to_date_string = (
            to_date.strftime(date_string_format)
            if isinstance(to_date, datetime)
            else to_date
        )
        url_args = {"instrument_token": instrument_token, "interval": interval}
        params = {
            "from": from_date_string,
            "to": to_date_string,
            "interval": interval,
            "continuous": 1 if continuous else 0,
            "oi": 1 if oi else 0,
        }
        if return_kwargs:
            return url_args, params
        if not web_api:
            data = self._get(
                "market.historical",
                url_args=url_args,
                params=params,
            )
            return self._format_historical(data, return_df=return_df)
        else:
            future = asyncio.run_coroutine_threadsafe(
                self.__feth_historical_data_from_web(
                    self._routes["web.historical"].format(**url_args),
                    params,
                    return_df=return_df,
                ),
                self.loop,
            )
            try:
                result = future.result(timeout=float(self._default_timeout * 8))
            except asyncio.TimeoutError:
                error_message = f"The Request Took Longer Than The Default Timeout To Wait For The Response, i.e. {self._default_timeout} Seconds, Cancelling The Task..."
                log.error(error_message)
                del error_message
                future.cancel()
            except Exception as exc:
                log.exception(f"The Request Ended Up With An Exception: {exc!r}")
            else:
                return result

    def _format_historical(
        self,
        data: Dict[str, List[List[Union[str, int, float, datetime]]]],
        return_df: bool = False,
    ) -> Union[pd.DataFrame, Dict[str, List[List[Union[str, int, float, datetime]]]]]:
        records = [
            {
                "date": dateutil.parser.parse(candle[0]),
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[5],
            }
            | {"oi": candle[6]}
            if len(candle) == 7
            else {}
            for candle in data["candles"]
        ]
        return pd.json_normalize(records) if return_df else records

    def multiple_historical_data(
        self,
        list_of_kwargs: List[Dict[str, Any]],
        return_df: bool = False,
        return_token_wise_df: bool = False,
    ) -> Optional[
        Union[
            Dict[int, Union[Dict[str, Any], pd.DataFrame, None]],
            List[Union[Dict[str, Any], pd.DataFrame, None]],
        ]
    ]:
        url_args_params = []
        for kwargs in list_of_kwargs:
            url_args_params.append(
                (
                    kwargs["instrument_token"],
                    self.historical_data(
                        instrument_token=kwargs["instrument_token"],
                        from_date=kwargs["from_date"],
                        to_date=kwargs["to_date"],
                        interval=kwargs["interval"],
                        continuous=kwargs["continuous"],
                        oi=kwargs["oi"],
                        web_api=True,
                        return_kwargs=True,
                    ),
                )
            )
        limit = asyncio.Semaphore(len(url_args_params))
        futures = asyncio.run_coroutine_threadsafe(
            self.__feth_Multiple_historical_data_from_web(
                url_args_params,
                limit,
                return_df=return_df,
                return_token_wise_df=return_token_wise_df,
            ),
            self.loop,
        )
        try:
            results = futures.result(
                timeout=float(self._default_timeout * 8 * len(url_args_params))
            )
        except asyncio.TimeoutError:
            error_message = f"The Requests Took Longer Than The Default Timeout To Wait For The Response, i.e. {self._default_timeout} Seconds, Cancelling The Task..."
            log.error(error_message)
            del error_message
            futures.cancel()
        except Exception as exc:
            log.exception(f"The Requests Ended Up With An Exception: {exc!r}")
        else:
            return results

    async def __request(
        self,
        url: str,
        method: str,
        params: Optional[Dict[str, Union[List[Any], Any]]] = None,
        data: Optional[Dict[str, Union[List[Any], Any]]] = None,
        json: Optional[Dict[str, Union[List[Any], Any]]] = None,
        verify_ssl: bool = True,
        allow_redirects: bool = True,
        proxy: Optional[str] = None,
        limit: asyncio.Semaphore = asyncio.Semaphore(2),
    ) -> Optional[Union[Dict[str, Union[List[Any], Any]], bytes]]:
        async with limit:
            try:
                response = await self.__reqsession.request(
                    method=method,
                    url=url,
                    json=json,
                    data=data,
                    params=params,
                    verify_ssl=verify_ssl,
                    allow_redirects=allow_redirects,
                    proxy=proxy if isinstance(proxy, str) else None,
                )
            except aiohttp.ClientError as err:
                raise err
            except asyncio.TimeoutError:
                return await self.__request(
                    url,
                    method,
                    json=json,
                    data=data,
                    params=params,
                    verify_ssl=verify_ssl,
                    allow_redirects=allow_redirects,
                    proxy=proxy,
                    limit=limit,
                )
            else:
                if response.headers.get("content-type") == "application/json":
                    try:
                        json_body = await response.json()
                    except (JSONDecodeError, ValueError):
                        content = await response.text()
                        raise ex.DataException(
                            "Could not parse the JSON response received from the server: {content}".format(
                                content=content
                            )
                        )
                    else:
                        if (
                            isinstance(json_body, dict)
                            and json_body.get("error_type") is not None
                            and json_body.get("status") == "error"
                        ):
                            # Call session hook if its registered and
                            # TokenException is raised.
                            if (
                                self.session_expiry_hook
                                and response.status == 403
                                and json_body["error_type"] == "TokenException"
                            ):
                                self.session_expiry_hook()
                            # native Kite errors
                            exp = getattr(
                                ex, json_body.get("error_type"), ex.GeneralException
                            )
                            raise exp(json_body["message"], code=response.status)
                        return json_body["data"]
                elif response.headers.get("content-type") == "csv":
                    return await response.read()
                else:
                    content = await response.text()
                    raise ex.DataException(
                        "Unknown Content-Type ({content_type}) with response: ({content})".format(
                            content_type=response.headers.get("content-type"),
                            content=content,
                        )
                    )

    def _request(
        self,
        route: str,
        method: str,
        url_args: Optional[Dict[str, Union[List[Any], Any]]] = None,
        params: Optional[Dict[str, Union[List[Any], Any]]] = None,
        query_params: Optional[Dict[str, Union[List[Any], Any]]] = None,
        is_json: Union[bool, List[bool]] = False,
    ) -> Optional[Union[Dict[str, Union[List[Any], Any]], bytes]]:
        """Make an HTTP request."""
        # Form a restful URL
        if url_args:
            uri = self._routes[route].format(**url_args)
        else:
            uri = self._routes[route]

        if method in self.__get_delete_method:
            query_params = params

        future = asyncio.run_coroutine_threadsafe(
            self.__request(
                url=uri,
                method=method,
                json=params if is_json and method in self.__post_put_method else None,
                data=params
                if not is_json and method in self.__post_put_method
                else None,
                params=query_params,
                verify_ssl=not self.disable_ssl,
                allow_redirects=True,
                proxy=self.proxies,
            ),
            self.loop,
        )
        try:
            result = future.result(timeout=float(self._default_timeout))
        except asyncio.TimeoutError:
            error_message = f"The Request Took Longer Than The Default Timeout To Wait For The Response, i.e. {self._default_timeout} Seconds, Cancelling The Task..."
            log.error(error_message)
            future.cancel()
        except Exception as exc:
            log.exception(f"The Request Ended Up With An Exception: {exc!r}")
        else:
            return result
