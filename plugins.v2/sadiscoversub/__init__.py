"""
接收神医的媒体订阅请求并创建订阅。
"""

import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.chain.subscribe import SubscribeChain
from app.core.event import Event, eventmanager
from app.core.metainfo import MetaInfo
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType

__all__ = ["SaDiscoverSub"]


class SaDiscoverSub(_PluginBase):
    """将神医 Emby 探索频道中的 library.subscribe 通知转换为订阅。"""

    plugin_name = "神医媒体订阅请求"
    plugin_desc = "接收神医的媒体订阅请求并创建订阅。"
    plugin_icon = "https://raw.githubusercontent.com/sjtuross/MoviePilot-Plugins/main/icons/sadiscoversub.png"
    plugin_version = "0.1.2"
    plugin_author = "sjtuross"
    author_url = "https://github.com/sjtuross"
    plugin_config_prefix = "sadiscoversub_"
    plugin_order = 50
    auth_level = 1

    _enabled = False
    _dedupe_seconds = 300
    _message = True
    _history: List[Dict[str, Any]] = []

    def init_plugin(self, config: Optional[Dict[str, Any]] = None) -> None:
        """读取配置并恢复最近事件记录。"""
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        try:
            self._dedupe_seconds = max(0, int(config.get("dedupe_seconds") or 300))
        except (TypeError, ValueError):
            self._dedupe_seconds = 300
        self._message = bool(config.get("message", True))
        history = self.get_data("history")
        self._history = history if isinstance(history, list) else []
        if self._enabled:
            self._warn_if_no_media_server()

    def _warn_if_no_media_server(self) -> None:
        """提示未配置媒体服务器时内置 Webhook 不会分发事件。"""
        try:
            if not MediaServerHelper().get_services():
                logger.warning("未配置或未启用媒体服务器")
        except Exception as error:
            logger.debug(f"检查媒体服务器配置失败：{error}")

    def get_state(self) -> bool:
        """返回插件是否已启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表；本插件只使用内置 Webhook。"""
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单及默认配置。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "color": "primary",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "message",
                                            "label": "创建订阅后发送通知",
                                            "color": "primary",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "dedupe_seconds",
                                            "label": "重复事件忽略窗口（秒）",
                                            "type": "number",
                                            "min": 0,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "在Emby中启用神医的媒体订阅请求Webhook通知，请求内容类型设为multipart/form-data",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "dedupe_seconds": 300,
            "message": True,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回最近收到的订阅事件及创建结果。"""
        if not self._history:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无事件记录。",
                    },
                }
            ]

        contents = []
        for entry in reversed(self._history):
            season = entry.get("season")
            season_text = f"S{season}" if season else "-"
            text = (
                f"{entry.get('time') or ''} | {entry.get('title') or ''} | "
                f"{entry.get('identity') or ''} | {season_text} | {entry.get('status') or ''}"
            )
            contents.append({
                "component": "VCardText",
                "text": text,
            })

        return [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": "最近事件",
                    },
                    *contents,
                ],
            }
        ]

    def stop_service(self) -> None:
        """停止插件运行状态。"""
        self._enabled = False

    @eventmanager.register(EventType.WebhookMessage)
    def on_webhook(self, event: Event) -> None:
        """处理媒体服务器 Webhook 并筛选 library.subscribe 事件。"""
        if not self._enabled:
            return

        info = getattr(event, "event_data", None)
        raw = getattr(info, "json_object", None) or {}
        if not isinstance(raw, dict):
            return
        if str(raw.get("Event") or "").strip().lower() != "library.subscribe":
            return

        item = raw.get("Item") or {}
        if not isinstance(item, dict):
            return

        logger.info(f"收到 library.subscribe 事件：{item.get('Name') or item.get('Id') or '未知条目'}")
        self._handle_subscribe_request(raw, item)

    @staticmethod
    def _resolve_media_type(discover_item_type: Optional[str]) -> Optional[MediaType]:
        """使用 DiscoverItemType 判断电影/电视剧；缺失时交由宿主识别。"""
        normalized_type = str(discover_item_type or "").strip().lower()
        if normalized_type == "tv":
            return MediaType.TV
        if normalized_type == "movie":
            return MediaType.MOVIE
        return None

    @staticmethod
    def _is_recognition_failure(message: str) -> bool:
        """判断订阅失败是否属于媒体身份未识别，可安全回退 TMDB。"""
        return "未识别到媒体信息" in str(message or "")

    def _create_subscription(
        self,
        title: str,
        year: str,
        detected_type: Optional[MediaType],
        season: Optional[int],
        media_source: str,
        media_id: str,
        username: Optional[str],
    ) -> Tuple[Optional[int], str]:
        """使用指定媒体身份创建订阅，异常转为返回结果。"""
        try:
            if media_source == "douban":
                return SubscribeChain().add(
                    title=title,
                    year=year,
                    mtype=detected_type,
                    season=season,
                    tmdbid=None,
                    doubanid=media_id,
                    username=username,
                    message=self._message,
                    exist_ok=True,
                )
            return SubscribeChain().add(
                title=title,
                year=year,
                mtype=detected_type,
                season=season,
                tmdbid=int(media_id),
                doubanid=None,
                username=username,
                message=self._message,
                exist_ok=True,
            )
        except Exception as error:
            logger.error(f"使用 {media_source}:{media_id} 创建订阅异常：{error}")
            return None, f"失败：{error}"

    def _handle_subscribe_request(self, raw: Dict[str, Any], item: Dict[str, Any]) -> None:
        """解析事件字段并调用订阅链创建订阅。"""
        provider_ids = item.get("ProviderIds") or {}
        if not isinstance(provider_ids, dict):
            provider_ids = {}
        normalized_ids = {
            str(key).lower(): str(value).strip()
            for key, value in provider_ids.items()
            if value is not None and str(value).strip()
        }

        tmdb_raw = normalized_ids.get("tmdb")
        douban_id = normalized_ids.get("douban")
        discover_item_type = normalized_ids.get("discoveritemtype")
        tmdb_id = int(tmdb_raw) if tmdb_raw and tmdb_raw.isdigit() else None
        if douban_id:
            media_source, media_id = "douban", douban_id
        elif tmdb_id:
            media_source, media_id = "tmdb", str(tmdb_id)
        else:
            self._record_event(item, tmdb_id, douban_id, "忽略：缺少 TMDB/豆瓣标识")
            logger.warning("library.subscribe 事件缺少 TMDB/豆瓣标识，已忽略")
            return

        title = str(item.get("Name") or "").strip()
        if not title:
            self._record_event(item, tmdb_id, douban_id, "忽略：缺少标题")
            logger.warning("library.subscribe 事件缺少标题，已忽略")
            return

        season = self._parse_season(title, item)
        detected_type = self._resolve_media_type(discover_item_type)
        year = str(item.get("ProductionYear") or "").strip()
        user = raw.get("User") or {}
        username = str(user.get("Name") or "").strip() or None
        identity = f"douban:{douban_id}" if douban_id else f"tmdb:{tmdb_id}"
        duplicate_key = f"{identity}:{season or ''}"

        season_log = f"season={season}，" if season else ""
        logger.info(
            f"订阅识别参数：title={title}，tmdb={tmdb_id or '-'}，douban={douban_id or '-'}，"
            f"DiscoverItemType={discover_item_type or '-'}，{season_log}"
            f"mtype={detected_type.value if detected_type else 'auto'}"
        )

        if self._is_duplicate(duplicate_key):
            self._record_event(item, tmdb_id, douban_id, "跳过：重复事件", season)
            logger.info(f"忽略重复的 library.subscribe 事件：{title} ({duplicate_key})")
            return

        used_source, used_id = media_source, media_id
        subscribe_id, result_message = self._create_subscription(
            title=title,
            year=year,
            detected_type=detected_type,
            season=season,
            media_source=media_source,
            media_id=media_id,
            username=username,
        )

        if (not subscribe_id and douban_id and tmdb_id
                and self._is_recognition_failure(result_message)):
            logger.warning(
                f"豆瓣 {douban_id} 未识别到媒体信息：{result_message}，改用 TMDB {tmdb_id} 重试"
            )
            used_source, used_id = "tmdb", str(tmdb_id)
            subscribe_id, result_message = self._create_subscription(
                title=title,
                year=year,
                detected_type=detected_type,
                season=season,
                media_source=used_source,
                media_id=used_id,
                username=username,
            )

        status = "已创建" if subscribe_id else "未新增"
        if result_message:
            status = f"{status}：{result_message}"
        used_identity = f"{used_source}:{used_id}"
        self._record_event(
            item, tmdb_id, douban_id, status, season,
            identity_override=used_identity,
        )
        logger.info(
            f"library.subscribe {status}：{title}，{used_identity}，订阅 ID：{subscribe_id}"
        )

    @staticmethod
    def _parse_season(title: str, item: Dict[str, Any]) -> Optional[int]:
        """从标题解析季号，无法解析时返回空。"""
        try:
            season = MetaInfo(title).begin_season
        except Exception:
            season = None

        if not season:
            match = re.search(r"(?i)\bS(?:eason)?\s*0*(\d{1,2})\b", title)
            if match:
                season = int(match.group(1))

        if not season:
            for key in ("ParentIndexNumber", "SeasonNumber"):
                value = item.get(key)
                if value is not None:
                    try:
                        season = int(value)
                    except (TypeError, ValueError):
                        season = None
                    if season:
                        break

        return season if season and season > 0 else None

    def _is_duplicate(self, duplicate_key: str) -> bool:
        """判断同一媒体是否仍处于去重窗口内。"""
        if self._dedupe_seconds <= 0:
            return False

        cutoff = time.time() - self._dedupe_seconds
        for entry in self._history:
            try:
                if entry.get("key") == duplicate_key and float(entry.get("ts") or 0) >= cutoff:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def _record_event(
        self,
        item: Dict[str, Any],
        tmdb_id: Optional[int],
        douban_id: Optional[str],
        status: str,
        season: Optional[int] = None,
        identity_override: Optional[str] = None,
    ) -> None:
        """把一次处理结果写入插件历史记录。"""
        now = time.time()
        title = str(item.get("Name") or "").strip()
        if identity_override:
            identity = identity_override
        elif douban_id:
            identity = f"douban:{douban_id}"
        elif tmdb_id:
            identity = f"tmdb:{tmdb_id}"
        else:
            identity = ""
        entry = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ts": now,
            "key": f"{identity}:{season or ''}",
            "title": title,
            "identity": identity,
            "season": season,
            "status": status,
        }
        self._history.append(entry)
        self._history = self._history[-100:]
        try:
            self.save_data("history", self._history)
        except Exception as error:
            logger.error(f"保存 library.subscribe 历史记录失败：{error}")
