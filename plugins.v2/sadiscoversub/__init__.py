"""
接收神医的媒体订阅请求并按豆瓣想看方式识别、查重、搜索下载或创建订阅。
"""

import re
import time
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.search import SearchChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.core.metainfo import MetaInfo
from app.db.subscribe_oper import SubscribeOper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType, SystemConfigKey

__all__ = ["SaDiscoverSub"]

_ACTION_LABELS = {
    "download": "下载",
    "subscribe": "订阅",
    "exist": "已存在",
    "failed": "失败",
}

_SEASON_SUFFIX_PATTERN = re.compile(
    r"(?i)(?:\s*第\s*[一二三四五六七八九十百\d]+\s*季|\s*\bS(?:eason)?\s*0*\d{1,2}\b)\s*$"
)


def _compact_title(text: Any) -> str:
    """标题归一化比较用：去掉标点空白并统一小写。"""
    if not text:
        return ""
    return "".join(ch for ch in str(text).casefold() if ch.isalnum())


def _strip_season_suffix(title: Any, season: Optional[int] = None) -> str:
    """去除标题尾部的季号后缀，提取用于一致性比对的基础片名。"""
    text = str(title or "").strip()
    if not text:
        return ""
    stripped = _SEASON_SUFFIX_PATTERN.sub("", text).strip()
    if season and season >= 2:
        tail_match = re.match(rf"^(.+?)\s*{season}$", stripped)
        if tail_match and len(tail_match.group(1).strip()) >= 2:
            stripped = tail_match.group(1).strip()
    return stripped or text


def _collect_media_titles(mediainfo: Any) -> set:
    """收集 MediaInfo 上所有可用于片名核对的归一化标题与别名。"""
    titles = set()
    if not mediainfo:
        return titles
    for attr in ("title", "original_title", "en_title", "hk_title", "tw_title", "sg_title"):
        val = _compact_title(getattr(mediainfo, attr, None))
        if val:
            titles.add(val)
    for alias in getattr(mediainfo, "names", None) or []:
        val = _compact_title(alias)
        if val:
            titles.add(val)
    tmdb_info = getattr(mediainfo, "tmdb_info", None)
    if isinstance(tmdb_info, dict):
        for key in ("title", "name", "original_title", "original_name"):
            val = _compact_title(tmdb_info.get(key))
            if val:
                titles.add(val)
    return titles


def _extract_year_int(year: Any) -> Optional[int]:
    """提取四位年份整数，格式非法时返回 None。"""
    text = str(year or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def _is_year_compatible(
    mediainfo: Any,
    expected_year: Any,
    season: Optional[int] = None,
) -> bool:
    """校验候选媒体年份是否兼容（剧集第 2 季及以上跳过首播年校验，其余允许 ±1 年误差）。"""
    if getattr(mediainfo, "type", None) == MediaType.TV and season and season >= 2:
        return True
    exp = _extract_year_int(expected_year)
    got = _extract_year_int(getattr(mediainfo, "year", None))
    if exp is None or got is None:
        return True
    return abs(got - exp) <= 1


def _matches_expected_titles(mediainfo: Any, expected_titles: List[str]) -> bool:
    """判断候选媒体标题/译名集合是否命中任意预期片名。"""
    expected = {_compact_title(t) for t in expected_titles if _compact_title(t)}
    if not expected:
        return True
    return bool(_collect_media_titles(mediainfo) & expected)


def _is_candidate_consistent(
    mediainfo: Any,
    expected_titles: List[str],
    expected_type: Optional[MediaType],
    expected_year: Any,
    season: Optional[int] = None,
) -> bool:
    """综合校验候选媒体的类型、年份与片名是否与请求一致。"""
    if not mediainfo:
        return False
    if expected_type and getattr(mediainfo, "type", None) and mediainfo.type != expected_type:
        return False
    if not _is_year_compatible(mediainfo, expected_year, season=season):
        return False
    return _matches_expected_titles(mediainfo, expected_titles)


class SaDiscoverSub(_PluginBase):
    """将神医 Emby 探索频道中的 library.subscribe 通知转换为下载或订阅。"""

    plugin_name = "神医媒体订阅请求"
    plugin_desc = "接收神医的媒体订阅请求，支持豆瓣转TMDB识别、媒体库查重与搜索下载。"
    plugin_icon = "https://raw.githubusercontent.com/sjtuross/MoviePilot-Plugins/main/icons/sadiscoversub.png"
    plugin_version = "0.2.0"
    plugin_author = "sjtuross"
    author_url = "https://github.com/sjtuross"
    plugin_config_prefix = "sadiscoversub_"
    plugin_order = 50
    auth_level = 1

    _enabled = False
    _dedupe_seconds = 300
    _message = True
    _search_download = False
    _clear = False
    _history: List[Dict[str, Any]] = []
    _lock = Lock()

    def init_plugin(self, config: Optional[Dict[str, Any]] = None) -> None:
        """读取配置并恢复最近事件记录。"""
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        raw_dedupe = config.get("dedupe_seconds")
        try:
            self._dedupe_seconds = 300 if raw_dedupe in (None, "") else max(0, int(raw_dedupe))
        except (TypeError, ValueError):
            self._dedupe_seconds = 300
        self._message = bool(config.get("message", True))
        self._search_download = bool(config.get("search_download", False))
        self._clear = bool(config.get("clear", False))

        if self._clear:
            self._history = []
            self.save_data("history", [])
            self._clear = False
            self.__update_config()
        else:
            history = self.get_data("history")
            self._history = list(history) if isinstance(history, list) else []

        if self._enabled:
            self._warn_if_no_media_server()

    def __update_config(self) -> None:
        """保存当前插件配置。"""
        self.update_config({
            "enabled": self._enabled,
            "dedupe_seconds": self._dedupe_seconds,
            "message": self._message,
            "search_download": self._search_download,
            "clear": self._clear,
        })

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
        """注册历史记录管理 API。"""
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除订阅处理历史记录",
            }
        ]

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
                                            "label": "发送订阅处理通知",
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
                                            "model": "search_download",
                                            "label": "搜索下载",
                                            "color": "primary",
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
                                "props": {"cols": 12, "md": 6},
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
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6,
                                    "style": "display:flex;align-items:center;",
                                },
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear",
                                            "label": "清理历史记录",
                                            "color": "primary",
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
                                            "text": "搜索下载开启后，会优先按订阅优先级规则组搜索过滤下载，搜索站点为设置的订阅站点，下载失败/无资源/剧集不完整时仍会添加订阅",
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
            "search_download": False,
            "clear": False,
        }

    def _load_history(self) -> List[Dict[str, Any]]:
        """读取最新历史记录，兼容未初始化数据层的测试场景。"""
        try:
            stored = self.get_data("history")
            if isinstance(stored, list):
                self._history = stored
        except Exception:
            pass
        return list(self._history)

    @staticmethod
    def _build_entry_link(entry: Dict[str, Any]) -> Optional[str]:
        """为详情页标题生成豆瓣或 TMDB 跳转链接。"""
        douban_id = entry.get("doubanid")
        if douban_id:
            return f"https://movie.douban.com/subject/{douban_id}"
        tmdb_id = entry.get("tmdbid")
        if tmdb_id:
            mtype = str(entry.get("type") or "")
            segment = "tv" if mtype == MediaType.TV.value or entry.get("season") else "movie"
            return f"https://www.themoviedb.org/{segment}/{tmdb_id}"
        return None

    @staticmethod
    def _entry_record_id(entry: Dict[str, Any]) -> str:
        """获取单条历史记录的稳定删除标识，兼容旧记录。"""
        return str(
            entry.get("id")
            or entry.get("key")
            or f"{entry.get('ts') or entry.get('time') or ''}:{entry.get('title') or ''}"
        )

    def get_page(self) -> List[dict]:
        """以海报卡片网格展示最近处理记录，并支持单条删除。"""
        historys = self._load_history()
        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {
                        "class": "text-center",
                    },
                }
            ]

        historys = sorted(
            historys,
            key=lambda x: (float(x.get("ts") or 0), str(x.get("time") or "")),
            reverse=True,
        )
        contents = []
        for entry in historys:
            title = str(entry.get("title") or "未知标题")
            poster = entry.get("poster")
            mtype = str(entry.get("type") or "-")
            season = entry.get("season")
            type_text = f"{mtype} S{int(season):02d}" if season else mtype
            time_str = str(entry.get("time") or "")
            identity = str(entry.get("identity") or "")
            status_text = str(
                entry.get("status")
                or _ACTION_LABELS.get(str(entry.get("action") or ""), entry.get("action") or "")
            )
            record_id = self._entry_record_id(entry)
            link = self._build_entry_link(entry)

            title_node: Dict[str, Any] = {
                "component": "VCardTitle",
                "props": {
                    "class": "ps-1 pe-5 break-words whitespace-break-spaces",
                },
            }
            if link:
                title_node["content"] = [
                    {
                        "component": "a",
                        "props": {
                            "href": link,
                            "target": "_blank",
                        },
                        "text": title,
                    }
                ]
            else:
                title_node["text"] = title

            info_nodes: List[Dict[str, Any]] = [
                title_node,
                {
                    "component": "VCardText",
                    "props": {"class": "pa-0 px-2"},
                    "text": f"类型：{type_text}",
                },
            ]
            if identity:
                info_nodes.append({
                    "component": "VCardText",
                    "props": {"class": "pa-0 px-2"},
                    "text": f"身份：{identity}",
                })
            info_nodes.extend([
                {
                    "component": "VCardText",
                    "props": {"class": "pa-0 px-2"},
                    "text": f"时间：{time_str}",
                },
                {
                    "component": "VCardText",
                    "props": {"class": "pa-0 px-2"},
                    "text": f"结果：{status_text}",
                },
            ])

            row_children: List[Dict[str, Any]] = []
            if poster:
                row_children.append({
                    "component": "div",
                    "content": [
                        {
                            "component": "VImg",
                            "props": {
                                "src": poster,
                                "height": 120,
                                "width": 80,
                                "aspect-ratio": "2/3",
                                "class": "object-cover shadow ring-gray-500",
                                "cover": True,
                            },
                        }
                    ],
                })
            row_children.append({
                "component": "div",
                "content": info_nodes,
            })

            contents.append({
                "component": "VCard",
                "content": [
                    {
                        "component": "VDialogCloseBtn",
                        "props": {
                            "innerClass": "absolute top-0 right-0",
                        },
                        "events": {
                            "click": {
                                "api": "plugin/SaDiscoverSub/delete_history",
                                "method": "get",
                                "params": {
                                    "record_id": record_id,
                                    "apikey": settings.API_TOKEN,
                                },
                            }
                        },
                    },
                    {
                        "component": "div",
                        "props": {
                            "class": "d-flex justify-space-start flex-nowrap flex-row",
                        },
                        "content": row_children,
                    },
                ],
            })

        return [
            {
                "component": "div",
                "props": {
                    "class": "grid gap-3 grid-info-card",
                },
                "content": contents,
            }
        ]

    def delete_history(self, record_id: Optional[str] = None, apikey: str = "") -> schemas.Response:
        """删除单条订阅处理历史记录。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        if not record_id:
            return schemas.Response(success=False, message="缺少记录标识")

        with self._lock:
            historys = self._load_history()
            if not historys:
                return schemas.Response(success=False, message="未找到历史记录")
            remaining = [h for h in historys if self._entry_record_id(h) != record_id]
            if len(remaining) == len(historys):
                return schemas.Response(success=False, message="未找到对应历史记录")
            self._history = remaining
            self.save_data("history", remaining)
        return schemas.Response(success=True, message="删除成功")

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
        """使用 DiscoverItemType 判断电影/电视剧；缺失时由后续识别补全。"""
        normalized_type = str(discover_item_type or "").strip().lower()
        if normalized_type == "tv":
            return MediaType.TV
        if normalized_type == "movie":
            return MediaType.MOVIE
        return None

    @staticmethod
    def _is_tmdb_recognize_source() -> bool:
        """判断当前系统识别源是否为 TMDB。"""
        return str(getattr(settings, "RECOGNIZE_SOURCE", "") or "").strip().lower() == "themoviedb"

    @staticmethod
    def _safe_media_image(mediainfo: Any, method_name: str) -> Optional[str]:
        """安全读取媒体图片地址。"""
        if not mediainfo:
            return None
        getter = getattr(mediainfo, method_name, None)
        if callable(getter):
            try:
                return getter()
            except Exception:
                return None
        return None

    def _safe_recognize_media(
        self,
        meta: Any,
        tmdbid: Optional[int] = None,
        doubanid: Optional[str] = None,
    ) -> Any:
        """调用识别链并吞掉单次异常，便于继续执行回退策略。"""
        try:
            if tmdbid is not None:
                return self.chain.recognize_media(meta=meta, tmdbid=tmdbid)
            if doubanid:
                return self.chain.recognize_media(meta=meta, doubanid=doubanid)
        except Exception as error:
            identity = f"tmdb:{tmdbid}" if tmdbid is not None else f"douban:{doubanid}"
            logger.warning(f"识别媒体 {identity} 异常：{error}")
        return None

    def _fetch_douban_info(self, douban_id: str) -> Dict[str, Any]:
        """安全获取豆瓣条目详情。"""
        try:
            info = self.chain.douban_info(doubanid=douban_id) or {}
            return info if isinstance(info, dict) else {}
        except Exception as error:
            logger.warning(f"获取豆瓣条目 {douban_id} 基础信息失败：{error}")
            return {}

    @staticmethod
    def _build_localized_titles(
        title: str,
        meta: Any,
        douban_info: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """整理用于一致性校验与中文片名纠偏的候选片名列表（保持优先级并去重）。"""
        season = getattr(meta, "begin_season", None)
        raw_candidates: List[str] = [
            _strip_season_suffix(title, season),
            str(title or "").strip(),
            str(getattr(meta, "name", "") or "").strip(),
        ]
        if isinstance(douban_info, dict):
            db_title = str(douban_info.get("title") or "").strip()
            if db_title:
                raw_candidates.extend([_strip_season_suffix(db_title, season), db_title])
            for aka in douban_info.get("aka") or []:
                aka_text = str(aka or "").strip()
                if aka_text:
                    raw_candidates.append(_strip_season_suffix(aka_text, season))

        result: List[str] = []
        seen = set()
        for candidate in raw_candidates:
            compacted = _compact_title(candidate)
            if candidate and compacted and compacted not in seen:
                seen.add(compacted)
                result.append(candidate)
        return result

    def _disambiguate_converted_media(
        self,
        mediachain: MediaChain,
        meta: Any,
        title: str,
        douban_id: str,
        tmdb_id: Optional[int],
        converted_tmdb_id: int,
        converted_media: Any,
        douban_info: Optional[Dict[str, Any]],
    ) -> Tuple[Any, str]:
        """
        对豆瓣→TMDB 转换结果做片名一致性校验；若疑为同年份同名撞车，依次通过
        Webhook 自带 TMDB ID 与中文片名重搜 TMDB 进行纠偏。
        """
        season = getattr(meta, "begin_season", None)
        expected_type = getattr(meta, "type", None)
        expected_year = getattr(meta, "year", None)
        request_titles = self._build_localized_titles(title, meta)

        if _is_candidate_consistent(
            converted_media,
            request_titles,
            expected_type,
            expected_year,
            season=season,
        ):
            return converted_media, f"tmdb:{converted_tmdb_id}"

        # 仅在初步校验不一致时才补拉豆瓣详情，避免正常命中时产生额外请求
        if douban_info is None:
            douban_info = self._fetch_douban_info(douban_id)
        if douban_info and not expected_year and douban_info.get("year"):
            expected_year = str(douban_info.get("year")).strip()
            meta.year = expected_year
        localized_titles = self._build_localized_titles(title, meta, douban_info)

        if _is_candidate_consistent(
            converted_media,
            localized_titles,
            expected_type,
            expected_year,
            season=season,
        ):
            return converted_media, f"tmdb:{converted_tmdb_id}"

        logger.warning(
            f"豆瓣 {douban_id} 转 TMDB {converted_tmdb_id}（{getattr(converted_media, 'title', '')}）"
            f"与请求片名「{title}」不一致，尝试自动纠偏"
        )

        # 纠偏 1：校验 Webhook 自带的 TMDB ID
        if tmdb_id and tmdb_id != converted_tmdb_id:
            webhook_media = self._safe_recognize_media(meta=meta, tmdbid=tmdb_id)
            if _is_candidate_consistent(
                webhook_media,
                localized_titles,
                expected_type,
                expected_year,
                season=season,
            ):
                logger.info(
                    f"片名纠偏命中 Webhook TMDB {tmdb_id}："
                    f"{getattr(webhook_media, 'title_year', getattr(webhook_media, 'title', ''))}"
                )
                return webhook_media, f"tmdb:{tmdb_id}"

        # 纠偏 2：改用中文/本地化片名直接匹配 TMDB（绕过英文原名按上映日期降序撞车）
        original_compact = _compact_title(
            douban_info.get("original_title") if isinstance(douban_info, dict) else None
        )
        tried_ids = {converted_tmdb_id}
        if tmdb_id:
            tried_ids.add(tmdb_id)
        match_fn = getattr(mediachain, "match_tmdbinfo", None)
        if callable(match_fn):
            for search_title in localized_titles:
                if original_compact and _compact_title(search_title) == original_compact:
                    continue
                try:
                    matched = match_fn(
                        name=search_title,
                        year=expected_year,
                        mtype=expected_type,
                        season=season,
                    )
                except Exception as error:
                    logger.debug(f"按片名「{search_title}」匹配 TMDB 异常：{error}")
                    continue
                if not isinstance(matched, dict) or not matched.get("id"):
                    continue
                try:
                    alt_id = int(matched.get("id"))
                except (TypeError, ValueError):
                    continue
                if alt_id in tried_ids:
                    continue
                tried_ids.add(alt_id)
                alt_media = self._safe_recognize_media(meta=meta, tmdbid=alt_id)
                if _is_candidate_consistent(
                    alt_media,
                    localized_titles,
                    expected_type,
                    expected_year,
                    season=season,
                ):
                    logger.info(
                        f"片名纠偏命中 TMDB {alt_id}（关键词「{search_title}」）："
                        f"{getattr(alt_media, 'title_year', getattr(alt_media, 'title', ''))}"
                    )
                    return alt_media, f"tmdb:{alt_id}"

        # 若未找到更优中文命中（例如冷门外片在 TMDB 暂无中文译名），在类型与年份兼容前提下保留原结果
        return converted_media, f"tmdb:{converted_tmdb_id}"

    def _recognize_request_media(
        self,
        meta: Any,
        title: str,
        douban_id: Optional[str],
        tmdb_id: Optional[int],
    ) -> Tuple[Any, Optional[str]]:
        """
        参考豆瓣想看执行媒体识别：
        - 识别源为 TMDB 时：优先豆瓣 ID 转 TMDB（附片名一致性校验与自动纠偏）
          -> 失败回退 Webhook 自带 TMDB ID -> 最终回退豆瓣识别；
        - 识别源非 TMDB 时：优先豆瓣识别 -> 失败回退 Webhook 自带 TMDB ID。
        """
        mediachain = MediaChain()
        douban_info: Optional[Dict[str, Any]] = None

        if douban_id and (not getattr(meta, "type", None) or not getattr(meta, "year", None)):
            douban_info = self._fetch_douban_info(douban_id)
            if douban_info:
                if not getattr(meta, "type", None) and douban_info.get("type"):
                    meta.type = (
                        MediaType.MOVIE
                        if douban_info.get("type") == "movie"
                        else MediaType.TV
                    )
                if not getattr(meta, "year", None) and douban_info.get("year"):
                    meta.year = str(douban_info.get("year")).strip()

        if self._is_tmdb_recognize_source():
            converted_tmdb_id: Optional[int] = None
            if douban_id:
                try:
                    tmdbinfo = mediachain.get_tmdbinfo_by_doubanid(
                        doubanid=douban_id,
                        mtype=getattr(meta, "type", None),
                    )
                except Exception as error:
                    logger.warning(f"豆瓣 ID {douban_id} 转 TMDB 异常：{error}")
                    tmdbinfo = None

                if isinstance(tmdbinfo, dict) and tmdbinfo.get("id"):
                    try:
                        converted_tmdb_id = int(tmdbinfo.get("id"))
                    except (TypeError, ValueError):
                        converted_tmdb_id = None
                    if getattr(meta, "begin_season", None) is None and tmdbinfo.get("season"):
                        try:
                            projected_season = int(tmdbinfo.get("season"))
                            if projected_season > 0:
                                meta.begin_season = projected_season
                        except (TypeError, ValueError):
                            pass

                if converted_tmdb_id:
                    mediainfo = self._safe_recognize_media(meta=meta, tmdbid=converted_tmdb_id)
                    if mediainfo:
                        return self._disambiguate_converted_media(
                            mediachain=mediachain,
                            meta=meta,
                            title=title,
                            douban_id=douban_id,
                            tmdb_id=tmdb_id,
                            converted_tmdb_id=converted_tmdb_id,
                            converted_media=mediainfo,
                            douban_info=douban_info,
                        )
                    logger.warning(f"豆瓣 {douban_id} 转换得到 TMDB {converted_tmdb_id} 但未识别到媒体信息")
                else:
                    logger.warning(f"未能通过豆瓣 ID {douban_id} 获取到 TMDB 信息，标题：{title}")

            if tmdb_id and tmdb_id != converted_tmdb_id:
                if douban_id:
                    logger.warning(f"改用 Webhook 自带 TMDB {tmdb_id} 回退识别：{title}")
                mediainfo = self._safe_recognize_media(meta=meta, tmdbid=tmdb_id)
                if mediainfo:
                    return mediainfo, f"tmdb:{tmdb_id}"
                logger.warning(f"TMDB {tmdb_id} 未识别到媒体信息：{title}")

            if douban_id:
                logger.warning(f"尝试回退使用豆瓣 ID {douban_id} 识别：{title}")
                mediainfo = self._safe_recognize_media(meta=meta, doubanid=douban_id)
                if mediainfo:
                    return mediainfo, f"douban:{douban_id}"
                logger.warning(f"豆瓣 ID {douban_id} 未识别到媒体信息：{title}")

            return None, None

        if douban_id:
            mediainfo = self._safe_recognize_media(meta=meta, doubanid=douban_id)
            if mediainfo:
                return mediainfo, f"douban:{douban_id}"
            logger.warning(f"豆瓣 ID {douban_id} 未识别到媒体信息：{title}")

        if tmdb_id:
            mediainfo = self._safe_recognize_media(meta=meta, tmdbid=tmdb_id)
            if mediainfo:
                return mediainfo, f"tmdb:{tmdb_id}"
            logger.warning(f"TMDB {tmdb_id} 未识别到媒体信息：{title}")

        return None, None

    @staticmethod
    def _resolved_identity(mediainfo: Any, fallback_identity: str) -> str:
        """从识别结果提取展示用的媒体身份标识。"""
        if mediainfo:
            if getattr(mediainfo, "tmdb_id", None):
                return f"tmdb:{mediainfo.tmdb_id}"
            if getattr(mediainfo, "douban_id", None):
                return f"douban:{mediainfo.douban_id}"
        return fallback_identity

    @staticmethod
    def _format_display_title(
        title: str,
        year: str,
        mtype: Optional[MediaType],
        season: Optional[int],
        mediainfo: Any = None,
    ) -> str:
        """拼装通知标题中的片名、年份与季号。"""
        if mediainfo and getattr(mediainfo, "title_year", None):
            display = str(mediainfo.title_year)
            media_type = getattr(mediainfo, "type", mtype)
        else:
            display = f"{title} ({year})" if year else title
            media_type = mtype
        if media_type == MediaType.TV:
            display = f"{display} S{season or 1:02d}"
        return display

    def _add_subscribe(
        self,
        subscribechain: SubscribeChain,
        subscribeoper: SubscribeOper,
        mediainfo: Any,
        season: Optional[int],
        username: Optional[str],
    ) -> Tuple[Optional[int], str, bool]:
        """
        使用识别后的 mediainfo 创建订阅。
        返回 (subscribe_id, result_message, native_notified)。
        """
        effective_season = (season or 1) if mediainfo.type == MediaType.TV else None
        try:
            already_exists = subscribeoper.exists(
                tmdbid=getattr(mediainfo, "tmdb_id", None),
                doubanid=getattr(mediainfo, "douban_id", None),
                season=effective_season,
            )
        except Exception as error:
            logger.debug(f"检查订阅是否已存在失败，交由订阅链继续处理：{error}")
            already_exists = False

        if already_exists:
            return None, "订阅已存在", False

        try:
            subscribe_id, result_message = subscribechain.add(
                title=mediainfo.title,
                year=mediainfo.year,
                mtype=mediainfo.type,
                tmdbid=getattr(mediainfo, "tmdb_id", None),
                doubanid=getattr(mediainfo, "douban_id", None),
                season=season,
                exist_ok=True,
                username=username,
                message=self._message,
            )
            native_notified = bool(self._message and subscribe_id)
            return subscribe_id, str(result_message or ""), native_notified
        except Exception as error:
            logger.error(f"添加订阅异常：{error}")
            return None, f"失败：{error}", False

    def _notify_status(
        self,
        title: str,
        text: str,
        mediainfo: Any = None,
    ) -> None:
        """发送带海报图（如有）的插件状态通知。"""
        if not self._message:
            return
        image = (
            self._safe_media_image(mediainfo, "get_message_image")
            or self._safe_media_image(mediainfo, "get_poster_image")
        )
        self.post_message(
            mtype=NotificationType.Subscribe,
            title=title,
            text=text,
            image=image,
        )

    def _handle_subscribe_request(self, raw: Dict[str, Any], item: Dict[str, Any]) -> None:
        """解析事件字段，执行识别、查重、可选搜索下载与订阅创建。"""
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
        if not douban_id and not tmdb_id:
            self._record_event(
                item=item,
                duplicate_key="",
                identity="",
                status="忽略：缺少 TMDB/豆瓣标识",
                action="failed",
            )
            logger.warning("library.subscribe 事件缺少 TMDB/豆瓣标识，已忽略")
            return

        title = str(item.get("Name") or "").strip()
        if not title:
            raw_identity = f"douban:{douban_id}" if douban_id else f"tmdb:{tmdb_id}"
            self._record_event(
                item=item,
                duplicate_key=f"{raw_identity}:",
                identity=raw_identity,
                status="忽略：缺少标题",
                action="failed",
                douban_id=douban_id,
                tmdb_id=tmdb_id,
            )
            logger.warning("library.subscribe 事件缺少标题，已忽略")
            return

        season = self._parse_season(title, item)
        detected_type = self._resolve_media_type(discover_item_type)
        year = str(item.get("ProductionYear") or "").strip()
        user = raw.get("User") or {}
        username = str(user.get("Name") or "").strip() or None
        raw_identity = f"douban:{douban_id}" if douban_id else f"tmdb:{tmdb_id}"
        duplicate_key = f"{raw_identity}:{season or ''}"

        season_log = f"season={season}，" if season else ""
        logger.info(
            f"订阅识别参数：title={title}，tmdb={tmdb_id or '-'}，douban={douban_id or '-'}，"
            f"DiscoverItemType={discover_item_type or '-'}，{season_log}"
            f"mtype={detected_type.value if detected_type else 'auto'}"
        )

        if self._is_duplicate(duplicate_key):
            logger.info(f"忽略重复的 library.subscribe 事件：{title} ({duplicate_key})")
            return

        meta = MetaInfo(title=title)
        if year:
            meta.year = year
        if detected_type:
            meta.type = detected_type
        if season is not None:
            meta.begin_season = season
            if not getattr(meta, "type", None):
                meta.type = MediaType.TV

        mediainfo, matched_identity = self._recognize_request_media(
            meta=meta,
            title=title,
            douban_id=douban_id,
            tmdb_id=tmdb_id,
        )
        if not mediainfo:
            self._record_event(
                item=item,
                duplicate_key=duplicate_key,
                identity=raw_identity,
                status="未识别到媒体信息",
                action="failed",
                season=season,
                mtype=detected_type.value if detected_type else None,
                year=year,
                douban_id=douban_id,
                tmdb_id=tmdb_id,
            )
            display = self._format_display_title(title, year, detected_type, season)
            self._notify_status(
                title=f"{display} 未添加订阅",
                text=f"{raw_identity}\n未识别到媒体信息",
            )
            return

        if mediainfo.type == MediaType.MOVIE:
            meta.begin_season = None
        effective_season = meta.begin_season if mediainfo.type == MediaType.TV else None
        used_identity = self._resolved_identity(mediainfo, matched_identity or raw_identity)
        resolved_tmdb_id = getattr(mediainfo, "tmdb_id", None) or tmdb_id
        resolved_douban_id = douban_id or getattr(mediainfo, "douban_id", None)
        display = self._format_display_title(
            title, year, mediainfo.type, effective_season, mediainfo=mediainfo
        )

        downloadchain = DownloadChain()
        searchchain = SearchChain()
        subscribechain = SubscribeChain()
        subscribeoper = SubscribeOper()

        try:
            exist_flag, no_exists = downloadchain.get_no_exists_info(
                meta=meta,
                mediainfo=mediainfo,
            )
        except Exception as error:
            logger.warning(f"查询媒体库存在状态失败，按缺失继续处理：{error}")
            exist_flag, no_exists = False, {}

        action = "subscribe"
        status = ""
        subscribe_id: Optional[int] = None
        result_message = ""
        need_fallback_subscribe = False

        if exist_flag:
            logger.info(f"{mediainfo.title_year} 媒体库中已存在")
            action = "exist"
            status = "媒体库已存在"
            self._notify_status(
                title=f"{display} 媒体库中已存在",
                text=used_identity,
                mediainfo=mediainfo,
            )
        elif self._search_download:
            logger.info(f"媒体库中不存在或不完整，开启搜索下载，开始搜索 {mediainfo.title_year} 的资源...")
            try:
                filter_results = searchchain.process(
                    mediainfo=mediainfo,
                    no_exists=no_exists,
                    sites=self.systemconfig.get(SystemConfigKey.RssSites),
                    rule_groups=self.systemconfig.get(SystemConfigKey.SubscribeFilterRuleGroups),
                )
            except Exception as error:
                logger.error(f"搜索 {mediainfo.title_year} 资源异常，转入添加订阅：{error}")
                filter_results = None

            if filter_results:
                logger.info(f"找到符合条件的资源，开始下载 {mediainfo.title_year} ...")
                if mediainfo.type == MediaType.MOVIE:
                    try:
                        download_id = downloadchain.download_single(
                            context=filter_results[0],
                            username=username,
                        )
                    except Exception as error:
                        logger.error(f"下载电影 {mediainfo.title_year} 异常，转入添加订阅：{error}")
                        download_id = None
                    if download_id:
                        action = "download"
                        status = "已下载"
                    else:
                        logger.info(f"下载失败，添加订阅 {mediainfo.title_year} ...")
                        need_fallback_subscribe = True
                else:
                    try:
                        downloaded_list, no_exists = downloadchain.batch_download(
                            contexts=filter_results,
                            no_exists=no_exists,
                            username=username,
                        )
                    except Exception as error:
                        logger.error(f"批量下载剧集 {mediainfo.title_year} 异常，转入添加订阅：{error}")
                        downloaded_list = []
                        need_fallback_subscribe = True
                    if not need_fallback_subscribe and not no_exists:
                        action = "download"
                        status = "已下载"
                    elif not need_fallback_subscribe:
                        logger.info(f"下载失败或未下载完所有剧集，添加订阅 {mediainfo.title_year} ...")
                        subscribe_id, result_message, native_notified = self._add_subscribe(
                            subscribechain=subscribechain,
                            subscribeoper=subscribeoper,
                            mediainfo=mediainfo,
                            season=effective_season,
                            username=username,
                        )
                        if subscribe_id:
                            try:
                                subscribe = subscribeoper.get(subscribe_id)
                                if subscribe:
                                    logger.info(f"根据缺失剧集更新订阅信息 {mediainfo.title_year} ...")
                                    subscribechain.finish_subscribe_or_not(
                                        subscribe=subscribe,
                                        meta=meta,
                                        mediainfo=mediainfo,
                                        downloads=downloaded_list,
                                        lefts=no_exists,
                                    )
                            except Exception as error:
                                logger.warning(f"更新订阅缺失剧集状态失败：{error}")
                        action, status = self._finalize_subscribe_result(
                            subscribe_id=subscribe_id,
                            result_message=result_message,
                            native_notified=native_notified,
                            display=display,
                            used_identity=used_identity,
                            mediainfo=mediainfo,
                            partial_downloaded=bool(downloaded_list),
                        )
            else:
                logger.info(f"未找到符合条件资源，添加订阅 {mediainfo.title_year} ...")
                need_fallback_subscribe = True
        else:
            logger.info(f"媒体库中不存在或不完整，未开启搜索下载，添加订阅 {mediainfo.title_year} ...")
            need_fallback_subscribe = True

        if need_fallback_subscribe:
            subscribe_id, result_message, native_notified = self._add_subscribe(
                subscribechain=subscribechain,
                subscribeoper=subscribeoper,
                mediainfo=mediainfo,
                season=effective_season,
                username=username,
            )
            action, status = self._finalize_subscribe_result(
                subscribe_id=subscribe_id,
                result_message=result_message,
                native_notified=native_notified,
                display=display,
                used_identity=used_identity,
                mediainfo=mediainfo,
            )

        self._record_event(
            item=item,
            duplicate_key=duplicate_key,
            identity=used_identity,
            status=status,
            action=action,
            season=effective_season,
            title_override=getattr(mediainfo, "title", None) or title,
            mtype=mediainfo.type.value if getattr(mediainfo, "type", None) else None,
            year=str(getattr(mediainfo, "year", "") or year),
            poster=self._safe_media_image(mediainfo, "get_poster_image"),
            douban_id=resolved_douban_id,
            tmdb_id=resolved_tmdb_id,
        )
        logger.info(
            f"library.subscribe {status}：{getattr(mediainfo, 'title', title)}，"
            f"{used_identity}，订阅 ID：{subscribe_id}"
        )

    def _finalize_subscribe_result(
        self,
        subscribe_id: Optional[int],
        result_message: str,
        native_notified: bool,
        display: str,
        used_identity: str,
        mediainfo: Any,
        partial_downloaded: bool = False,
    ) -> Tuple[str, str]:
        """整理订阅结果状态并在原生未发通知时补发状态通知。"""
        if subscribe_id and ("新增订阅成功" in result_message or "已存在" not in result_message):
            status = "部分下载并已订阅" if partial_downloaded else "已添加订阅"
            return "subscribe", status

        if "已存在" in result_message or subscribe_id:
            status = "订阅已存在"
            if not native_notified:
                self._notify_status(
                    title=f"{display} 订阅已存在",
                    text=used_identity,
                    mediainfo=mediainfo,
                )
            return "subscribe", status

        status = f"未新增：{result_message}" if result_message else "未新增"
        if not native_notified:
            text = f"{used_identity}\n{result_message}" if result_message else used_identity
            self._notify_status(
                title=f"{display} 未添加订阅",
                text=text,
                mediainfo=mediainfo,
            )
        return "failed", status

    @staticmethod
    def _parse_season(title: str, item: Dict[str, Any]) -> Optional[int]:
        """从标题解析季号，无法解析时回退到条目季字段。"""
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
        duplicate_key: str,
        identity: str,
        status: str,
        action: str = "subscribe",
        season: Optional[int] = None,
        title_override: Optional[str] = None,
        mtype: Optional[str] = None,
        year: Optional[str] = None,
        poster: Optional[str] = None,
        douban_id: Optional[str] = None,
        tmdb_id: Optional[int] = None,
    ) -> None:
        """把一次处理结果写入插件历史记录。"""
        now = time.time()
        title = str(title_override or item.get("Name") or "").strip()
        record_id = f"{int(now * 1_000_000)}_{duplicate_key or identity}"
        entry = {
            "id": record_id,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ts": now,
            "key": duplicate_key,
            "title": title,
            "type": mtype,
            "year": year or "",
            "poster": poster,
            "identity": identity,
            "doubanid": douban_id,
            "tmdbid": tmdb_id,
            "season": season,
            "action": action,
            "status": status,
        }
        with self._lock:
            self._history.append(entry)
            self._history = self._history[-100:]
            try:
                self.save_data("history", self._history)
            except Exception as error:
                logger.error(f"保存 library.subscribe 历史记录失败：{error}")
