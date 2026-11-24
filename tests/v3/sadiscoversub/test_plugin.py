"""SaDiscoverSub 插件的纯逻辑与订阅链路测试。"""

import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pytest
from app.schemas.types import MediaSource, MediaType

import app.plugins.sadiscoversub as sadiscoversub_module
from app.plugins.sadiscoversub import SaDiscoverSub


class _StubMetaInfo:
    """只保留基础字段的元数据替身，隔离宿主配置依赖。"""

    def __init__(self, title: Any = None, **_: Any) -> None:
        self.title = str(title or "")
        self.name = self.title
        self.year: Optional[str] = None
        self.type: Optional[MediaType] = None
        self.begin_season: Optional[int] = None


class _StubMediaInfo:
    """模拟识别后的 MediaInfo。"""

    def __init__(
        self,
        title: str = "示例作品",
        year: str = "2025",
        mtype: MediaType = MediaType.MOVIE,
        media_source: MediaSource = MediaSource.TMDB,
        media_id: str = "100",
        poster: str = "https://example.org/poster.jpg",
    ) -> None:
        self.title = title
        self.year = year
        self.title_year = f"{title} ({year})" if year else title
        self.type = mtype
        self.media_source = media_source
        self.media_id = media_id
        self._poster = poster

    def get_poster_image(self) -> str:
        return self._poster

    def get_message_image(self) -> str:
        return self._poster


class _StubPluginChain:
    """记录豆瓣信息、TMDB 续集搜索和媒体识别调用。"""

    def __init__(
        self,
        recognize_map: Optional[Dict[Any, Any]] = None,
        douban_data: Optional[Dict[str, Any]] = None,
        candidates: Optional[List[Dict[str, Any]]] = None,
        season_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.recognize_map = recognize_map or {}
        self.douban_data = douban_data or {}
        self.candidates = candidates or []
        self.season_info = season_info or {}
        self.recognize_calls: List[Dict[str, Any]] = []
        self.search_calls: List[Dict[str, Any]] = []
        self.season_calls: List[Dict[str, Any]] = []

    def douban_info(self, doubanid: str) -> Dict[str, Any]:
        return dict(self.douban_data)

    def search_medias(self, meta: Any, media_source: Any = None) -> List[Any]:
        self.search_calls.append({"meta": meta, "media_source": media_source})
        return list(self.candidates)

    def tmdb_info(
        self,
        tmdbid: int,
        mtype: Any = None,
        season: Optional[int] = None,
    ) -> Any:
        self.season_calls.append({"tmdbid": tmdbid, "mtype": mtype, "season": season})
        return self.season_info

    def recognize_media(
        self,
        meta: Any = None,
        media_source: Any = None,
        media_id: Any = None,
    ) -> Any:
        self.recognize_calls.append({
            "meta": meta,
            "media_source": media_source,
            "media_id": str(media_id),
        })
        return self.recognize_map.get((media_source, str(media_id)))


def _build_plugin(
    chain: Optional[_StubPluginChain] = None,
    *,
    search_download: bool = False,
    message: bool = True,
) -> SaDiscoverSub:
    """构造不触发宿主存储与初始化的插件实例。"""
    plugin = object.__new__(SaDiscoverSub)
    plugin._enabled = True
    plugin._dedupe_seconds = 300
    plugin._message = message
    plugin._search_download = search_download
    plugin._clear = False
    plugin._history = []
    plugin.chain = chain or _StubPluginChain()
    plugin.systemconfig = SimpleNamespace(get=lambda key: [1, 2])
    plugin.get_data = Mock(side_effect=lambda key: list(plugin._history) if key == "history" else None)  # type: ignore[method-assign]
    plugin.save_data = Mock()  # type: ignore[method-assign]
    plugin.update_config = Mock()  # type: ignore[method-assign]
    plugin.post_message = Mock()  # type: ignore[method-assign]
    return plugin


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    converted: Optional[Dict[str, Any]] = None,
    exist_flag: bool = False,
    no_exists: Optional[Dict[str, Any]] = None,
    search_results: Optional[List[Any]] = None,
    download_single_id: Optional[str] = "dl-1",
    batch_result: Optional[Any] = None,
    subscribe_result: Any = (101, "新增订阅成功"),
    recognize_source: str = "themoviedb",
) -> Dict[str, Any]:
    """注入离线链桩，收集每次关键调用。"""
    records: Dict[str, Any] = {
        "convert_calls": [],
        "search_calls": [],
        "download_single_calls": [],
        "batch_download_calls": [],
        "subscribe_add_calls": [],
        "finish_calls": [],
    }

    class _MediaChain:
        def convert_media_identity(self, **kwargs: Any) -> Optional[Dict[str, Any]]:
            records["convert_calls"].append(kwargs)
            return converted

    class _DownloadChain:
        def get_no_exists_info(self, meta: Any, mediainfo: Any) -> Any:
            return exist_flag, dict(no_exists or {})

        def download_single(self, context: Any, username: Any = None) -> Optional[str]:
            records["download_single_calls"].append({"context": context, "username": username})
            return download_single_id

        def batch_download(self, contexts: Any, no_exists: Any = None, username: Any = None) -> Any:
            records["batch_download_calls"].append({
                "contexts": contexts,
                "no_exists": no_exists,
                "username": username,
            })
            if batch_result is not None:
                return batch_result
            return [], {}

    class _SearchChain:
        def process(self, **kwargs: Any) -> Any:
            records["search_calls"].append(kwargs)
            return search_results

    class _SubscribeChain:
        def add(self, **kwargs: Any) -> Any:
            records["subscribe_add_calls"].append(kwargs)
            return subscribe_result

        def finish_subscribe_or_not(self, **kwargs: Any) -> None:
            records["finish_calls"].append(kwargs)

    class _SubscribeOper:
        def get(self, sub_id: int) -> Any:
            return SimpleNamespace(id=sub_id)

    monkeypatch.setattr(sadiscoversub_module, "MetaInfo", _StubMetaInfo)
    monkeypatch.setattr(sadiscoversub_module, "MediaChain", _MediaChain)
    monkeypatch.setattr(sadiscoversub_module, "DownloadChain", _DownloadChain)
    monkeypatch.setattr(sadiscoversub_module, "SearchChain", _SearchChain)
    monkeypatch.setattr(sadiscoversub_module, "SubscribeChain", _SubscribeChain)
    monkeypatch.setattr(sadiscoversub_module, "SubscribeOper", _SubscribeOper)
    monkeypatch.setattr(
        sadiscoversub_module,
        "settings",
        SimpleNamespace(RECOGNIZE_SOURCE=recognize_source, API_TOKEN="secret-token"),
    )
    return records


def test_parse_season_from_chinese_title() -> None:
    """从中文季标题解析季号。"""
    assert SaDiscoverSub._parse_season("庆余年 第二季", {}) == 2


def test_parse_season_from_english_title() -> None:
    """从英文季标题解析季号。"""
    assert SaDiscoverSub._parse_season("某剧 Season 2", {}) == 2
    assert SaDiscoverSub._parse_season("某剧 S03", {}) == 3


def test_parse_season_without_season() -> None:
    """电影标题不应被误判为剧集季号。"""
    assert SaDiscoverSub._parse_season("人生若如初见", {}) is None


def test_parse_season_falls_back_to_item_fields() -> None:
    """标题无法解析时回退到条目季字段。"""
    assert SaDiscoverSub._parse_season("某剧", {"ParentIndexNumber": 4}) == 4


def test_duplicate_window_uses_media_identity() -> None:
    """同一媒体身份在去重窗口内应被识别为重复。"""
    plugin = object.__new__(SaDiscoverSub)
    plugin._dedupe_seconds = 60
    plugin._history = [{"key": "themoviedb:206219:", "ts": time.time()}]

    assert plugin._is_duplicate("themoviedb:206219:")
    assert not plugin._is_duplicate("themoviedb:206220:")


def test_duplicate_window_expires() -> None:
    """超过去重窗口后应允许再次处理。"""
    plugin = object.__new__(SaDiscoverSub)
    plugin._dedupe_seconds = 1
    plugin._history = [{"key": "themoviedb:206219:", "ts": time.time() - 5}]

    assert not plugin._is_duplicate("themoviedb:206219:")


def test_init_plugin_clears_history_and_allows_zero_dedupe() -> None:
    """clear=True 应立即清空历史并复位配置，dedupe_seconds=0 应关闭去重。"""
    plugin = _build_plugin()
    plugin._history = [{"id": "old", "key": "douban:1:", "ts": time.time()}]
    plugin._warn_if_no_media_server = Mock()  # type: ignore[method-assign]

    plugin.init_plugin({
        "enabled": True,
        "dedupe_seconds": 0,
        "search_download": True,
        "clear": True,
    })

    assert plugin._dedupe_seconds == 0
    assert plugin._search_download is True
    assert plugin._clear is False
    assert plugin._history == []
    plugin.save_data.assert_called_once_with("history", [])
    plugin.update_config.assert_called_once()


def test_douban_converts_to_tmdb_and_uses_native_subscribe_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TMDB 识别源下优先由豆瓣转 TMDB 识别并创建订阅，新增订阅不重复发送插件通知。"""
    media = _StubMediaInfo(title="流浪地球", year="2019", mtype=MediaType.MOVIE, media_id="535167")
    chain = _StubPluginChain(recognize_map={(MediaSource.TMDB, "535167"): media})
    records = _install_runtime(monkeypatch, converted={"id": 535167})
    plugin = _build_plugin(chain)

    plugin._handle_subscribe_request(
        raw={"User": {"Name": "alice"}},
        item={
            "Name": "流浪地球",
            "ProductionYear": 2019,
            "ProviderIds": {"Douban": "26266893", "Tmdb": "999999", "DiscoverItemType": "movie"},
        },
    )

    assert len(records["convert_calls"]) == 1
    assert chain.recognize_calls[0]["media_source"] == MediaSource.TMDB
    assert chain.recognize_calls[0]["media_id"] == "535167"
    assert len(records["subscribe_add_calls"]) == 1
    sub_call = records["subscribe_add_calls"][0]
    assert sub_call["media_source"] == MediaSource.TMDB
    assert sub_call["media_id"] == "535167"
    assert sub_call["message"] is True
    assert sub_call["username"] == "alice"
    # 新增订阅由宿主发送原生图文通知，插件不重复发通知
    plugin.post_message.assert_not_called()
    assert plugin._history[-1]["action"] == "subscribe"
    assert plugin._history[-1]["status"] == "已添加订阅"


def test_fallback_order_webhook_tmdb_then_douban(monkeypatch: pytest.MonkeyPatch) -> None:
    """豆瓣转 TMDB 失败时，先回退 Webhook 自带 TMDB，再回退豆瓣识别。"""
    media_from_webhook = _StubMediaInfo(title="测试剧", media_source=MediaSource.TMDB, media_id="888")
    chain = _StubPluginChain(recognize_map={(MediaSource.TMDB, "888"): media_from_webhook})
    records = _install_runtime(monkeypatch, converted=None)
    plugin = _build_plugin(chain)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "测试剧",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "123456", "Tmdb": "888", "DiscoverItemType": "tv"},
        },
    )

    assert [(c["media_source"], c["media_id"]) for c in chain.recognize_calls] == [
        (MediaSource.TMDB, "888")
    ]
    assert records["subscribe_add_calls"][0]["media_id"] == "888"

    # Webhook TMDB 也识别失败时，最终回退到豆瓣识别
    media_from_douban = _StubMediaInfo(title="测试剧", media_source=MediaSource.Douban, media_id="123456")
    chain2 = _StubPluginChain(recognize_map={(MediaSource.Douban, "123456"): media_from_douban})
    records2 = _install_runtime(monkeypatch, converted=None)
    plugin2 = _build_plugin(chain2)

    plugin2._handle_subscribe_request(
        raw={},
        item={
            "Name": "测试剧",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "123456", "Tmdb": "888", "DiscoverItemType": "tv"},
        },
    )

    assert [(c["media_source"], c["media_id"]) for c in chain2.recognize_calls] == [
        (MediaSource.TMDB, "888"),
        (MediaSource.Douban, "123456"),
    ]
    assert records2["subscribe_add_calls"][0]["media_source"] == MediaSource.Douban
    assert records2["subscribe_add_calls"][0]["media_id"] == "123456"


def test_sequel_fallback_hits_tmdb_original_series(monkeypatch: pytest.MonkeyPatch) -> None:
    """豆瓣续集条目在身份转换失败后，命中唯一精确同名原剧与目标季年份时按原剧识别。"""
    media = _StubMediaInfo(title="示例剧集", year="2023", mtype=MediaType.TV, media_id="100")
    chain = _StubPluginChain(
        recognize_map={(MediaSource.TMDB, "100"): media},
        candidates=[{
            "type": "电视剧",
            "media_source": "themoviedb",
            "media_id": "100",
            "title": "示例剧集",
        }],
        season_info={"season_number": 2, "air_date": "2025-06-07", "episode_count": 10},
    )
    records = _install_runtime(monkeypatch, converted=None)
    plugin = _build_plugin(chain)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "示例剧集2",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "7654321", "DiscoverItemType": "tv"},
        },
    )

    assert chain.recognize_calls[0]["media_source"] == MediaSource.TMDB
    assert chain.recognize_calls[0]["media_id"] == "100"
    assert records["subscribe_add_calls"][0]["season"] == 2


def test_existing_in_library_skips_search_and_subscribe(monkeypatch: pytest.MonkeyPatch) -> None:
    """媒体库已存在时跳过搜索与订阅，记录 exist 并发送带海报通知。"""
    media = _StubMediaInfo(title="已入库电影", poster="https://example.org/p.jpg")
    chain = _StubPluginChain(recognize_map={(MediaSource.TMDB, "100"): media})
    records = _install_runtime(monkeypatch, converted={"id": 100}, exist_flag=True)
    plugin = _build_plugin(chain, search_download=True, message=True)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "已入库电影",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "111", "DiscoverItemType": "movie"},
        },
    )

    assert records["search_calls"] == []
    assert records["subscribe_add_calls"] == []
    assert plugin._history[-1]["action"] == "exist"
    assert plugin._history[-1]["status"] == "媒体库已存在"
    plugin.post_message.assert_called_once()
    assert plugin.post_message.call_args.kwargs["image"] == "https://example.org/p.jpg"


def test_search_download_movie_success_and_tv_partial_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """开启搜索下载时：电影下载成功不建订阅；剧集未下完时补建订阅并更新缺失集。"""
    movie = _StubMediaInfo(title="热映电影", mtype=MediaType.MOVIE, media_id="200")
    chain_movie = _StubPluginChain(recognize_map={(MediaSource.TMDB, "200"): movie})
    records_movie = _install_runtime(
        monkeypatch,
        converted={"id": 200},
        search_results=["ctx-1"],
        download_single_id="dl-200",
    )
    plugin_movie = _build_plugin(chain_movie, search_download=True)

    plugin_movie._handle_subscribe_request(
        raw={"User": {"Name": "bob"}},
        item={
            "Name": "热映电影",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "222", "DiscoverItemType": "movie"},
        },
    )

    assert len(records_movie["download_single_calls"]) == 1
    assert records_movie["subscribe_add_calls"] == []
    assert plugin_movie._history[-1]["action"] == "download"

    # 剧集只下载部分剧集，仍有 no_exists -> 补建订阅并调用 finish_subscribe_or_not
    tv = _StubMediaInfo(title="连载剧", mtype=MediaType.TV, media_id="300")
    chain_tv = _StubPluginChain(recognize_map={(MediaSource.TMDB, "300"): tv})
    records_tv = _install_runtime(
        monkeypatch,
        converted={"id": 300},
        search_results=["ctx-tv"],
        batch_result=(["ctx-tv"], {"themoviedb:300": {1: SimpleNamespace(episodes=[5, 6])}}),
        subscribe_result=(301, "新增订阅成功"),
    )
    plugin_tv = _build_plugin(chain_tv, search_download=True)

    plugin_tv._handle_subscribe_request(
        raw={},
        item={
            "Name": "连载剧",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "333", "DiscoverItemType": "tv"},
        },
    )

    assert len(records_tv["batch_download_calls"]) == 1
    assert len(records_tv["subscribe_add_calls"]) == 1
    assert len(records_tv["finish_calls"]) == 1
    assert plugin_tv._history[-1]["action"] == "subscribe"
    assert plugin_tv._history[-1]["status"] == "部分下载并已订阅"


def test_existing_subscription_sends_poster_status_notification(monkeypatch: pytest.MonkeyPatch) -> None:
    """订阅已存在时原生不发通知，插件应补发带海报图的状态通知。"""
    media = _StubMediaInfo(title="重复订阅", media_id="400", poster="https://example.org/400.jpg")
    chain = _StubPluginChain(recognize_map={(MediaSource.TMDB, "400"): media})
    _install_runtime(monkeypatch, converted={"id": 400}, subscribe_result=(401, "订阅已存在"))
    plugin = _build_plugin(chain, message=True)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "重复订阅",
            "ProductionYear": 2025,
            "ProviderIds": {"Douban": "444", "DiscoverItemType": "movie"},
        },
    )

    plugin.post_message.assert_called_once()
    assert "订阅已存在" in plugin.post_message.call_args.kwargs["title"]
    assert plugin.post_message.call_args.kwargs["image"] == "https://example.org/400.jpg"


def test_get_page_cards_and_delete_history_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """详情页应渲染海报卡片网格，delete_history 应校验令牌并精确删除记录。"""
    _install_runtime(monkeypatch)
    plugin = _build_plugin()
    plugin._history = [
        {
            "id": "rec-1",
            "time": "2026-11-24 12:00:00",
            "ts": 100.0,
            "key": "douban:111:",
            "title": "卡片电影",
            "type": "电影",
            "poster": "https://example.org/card.jpg",
            "identity": "themoviedb:100",
            "doubanid": "111",
            "action": "subscribe",
            "status": "已添加订阅",
        }
    ]

    page = plugin.get_page()
    assert page[0]["props"]["class"] == "grid gap-3 grid-info-card"
    card = page[0]["content"][0]
    assert card["component"] == "VCard"
    close_btn = card["content"][0]
    assert close_btn["events"]["click"]["api"] == "plugin/SaDiscoverSub/delete_history"
    assert close_btn["events"]["click"]["params"]["record_id"] == "rec-1"

    bad = plugin.delete_history(record_id="rec-1", apikey="wrong")
    assert bad.success is False
    ok = plugin.delete_history(record_id="rec-1", apikey="secret-token")
    assert ok.success is True
    assert plugin._history == []


def test_disambiguate_same_year_english_collision_via_webhook_tmdb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """豆瓣转 TMDB 撞车同名冷门片（如 Verity 2026）时，优先采信通过中文片名校验的 Webhook TMDB。"""
    wrong = _StubMediaInfo(title="Verity", year="2026", mtype=MediaType.MOVIE, media_id="1812260")
    correct = _StubMediaInfo(title="不存在的罪人", year="2026", mtype=MediaType.MOVIE, media_id="1389533")
    chain = _StubPluginChain(
        recognize_map={
            (MediaSource.TMDB, "1812260"): wrong,
            (MediaSource.TMDB, "1389533"): correct,
        },
        douban_data={"title": "不存在的罪人", "original_title": "Verity", "year": "2026"},
    )
    records = _install_runtime(monkeypatch, converted={"id": 1812260})
    plugin = _build_plugin(chain)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "不存在的罪人",
            "ProductionYear": 2026,
            "ProviderIds": {"Douban": "36900001", "Tmdb": "1389533", "DiscoverItemType": "movie"},
        },
    )

    assert records["subscribe_add_calls"][0]["media_id"] == "1389533"
    assert plugin._history[-1]["title"] == "不存在的罪人"
    assert plugin._history[-1]["identity"] == "themoviedb:1389533"


def test_disambiguate_same_year_english_collision_via_chinese_title_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Webhook 未带 TMDB 时，同名撞车应按中文片名重搜 TMDB 并纠偏到正确影片。"""
    wrong = _StubMediaInfo(title="Verity", year="2026", mtype=MediaType.MOVIE, media_id="1812260")
    correct = _StubMediaInfo(title="不存在的罪人", year="2026", mtype=MediaType.MOVIE, media_id="1389533")
    chain = _StubPluginChain(
        recognize_map={
            (MediaSource.TMDB, "1812260"): wrong,
            (MediaSource.TMDB, "1389533"): correct,
        },
        douban_data={"title": "不存在的罪人", "original_title": "Verity", "year": "2026"},
        candidates=[
            {
                "type": "电影",
                "media_source": "themoviedb",
                "media_id": "1389533",
                "title": "不存在的罪人",
                "original_title": "Verity",
                "year": "2026",
            }
        ],
    )
    records = _install_runtime(monkeypatch, converted={"id": 1812260}, exist_flag=True)
    plugin = _build_plugin(chain)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "不存在的罪人",
            "ProductionYear": 2026,
            "ProviderIds": {"Douban": "36900001", "DiscoverItemType": "movie"},
        },
    )

    assert len(chain.search_calls) == 1
    assert chain.search_calls[0]["meta"].title == "不存在的罪人"
    assert records["subscribe_add_calls"] == []
    assert plugin._history[-1]["action"] == "exist"
    assert plugin._history[-1]["identity"] == "themoviedb:1389533"


def test_disambiguate_keeps_converted_media_when_no_chinese_tmdb_translation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TMDB 确实暂无中文译名时，纠偏未命中应安全保留原豆瓣转 TMDB 结果。"""
    foreign = _StubMediaInfo(title="Obscure Film", year="2026", mtype=MediaType.MOVIE, media_id="555000")
    chain = _StubPluginChain(
        recognize_map={(MediaSource.TMDB, "555000"): foreign},
        douban_data={"title": "小众外片", "original_title": "Obscure Film", "year": "2026"},
        candidates=[],
    )
    records = _install_runtime(monkeypatch, converted={"id": 555000})
    plugin = _build_plugin(chain)

    plugin._handle_subscribe_request(
        raw={},
        item={
            "Name": "小众外片",
            "ProductionYear": 2026,
            "ProviderIds": {"Douban": "36900002", "DiscoverItemType": "movie"},
        },
    )

    assert records["subscribe_add_calls"][0]["media_id"] == "555000"

