#!/usr/bin/env python3
"""
CLIProxyAPI Client Rate Limiter & Concurrency Queue Realtime Dashboard
====================================================================
Pure Python standard library implementation with zero external dependencies.
Features:
- Accurate CJK character width calculation and ANSI-stripping alignment
- Flicker-free terminal refresh buffer with line/screen clearing
- Mathematically reconciled request accounting (Total = Admitted + Rejected + InQueue)
- Explicit separation between instantaneous realtime states and historical totals
- Adaptive box/compact terminal styles
"""

import argparse
import atexit
import json
import os
import re
import shutil
import sys
import time
import unicodedata
import urllib.error
import urllib.request

ANSI_REGEX = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")


def str_display_width(s: str) -> int:
    """Calculate visible display width of a string in terminal columns."""
    clean = ANSI_REGEX.sub("", s)
    return sum(
        2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in clean
    )


def pad(text: str, width: int, align: str = "left") -> str:
    """Pad a string (possibly containing ANSI color codes and CJK characters) to target display width."""
    dw = str_display_width(text)
    pad_len = max(0, width - dw)
    if align == "right":
        return " " * pad_len + text
    elif align == "center":
        left = pad_len // 2
        right = pad_len - left
        return " " * left + text + " " * right
    return text + " " * pad_len


def format_time_short(ms: float) -> str:
    """Format millisecond duration to human readable compact time string."""
    if not ms or ms <= 0:
        return "0ms"
    if ms < 1000:
        return f"{int(round(ms))}ms"
    return f"{ms / 1000.0:.1f}s"


def render_bar(in_flight: int, capacity: int, bar_width: int = 8) -> str:
    """Render a colored concurrency progress bar."""
    if capacity <= 0:
        return pad("N/A", bar_width)
    ratio = min(max(in_flight / capacity, 0.0), 1.0)
    filled = int(round(ratio * bar_width))
    empty = bar_width - filled

    if in_flight == 0:
        color = "\033[90m"  # Gray
    elif ratio >= 1.0:
        color = "\033[91m"  # Red
    elif ratio >= 0.6:
        color = "\033[93m"  # Yellow
    else:
        color = "\033[92m"  # Green

    bar = f"{color}{'█' * filled}{'░' * empty}\033[0m"
    return f"{bar} ({in_flight}/{capacity})"


def fetch_metrics(url: str, token: str):
    """Fetch metrics from the management plugin endpoint."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            return data.get("ratelimit_metrics", {}), data.get("timestamp", "")
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return None, f"Connection error: {e.reason}"
    except Exception as e:
        return None, str(e)


def build_frame(
    metrics: dict,
    ts: str,
    prev_totals: dict,
    elapsed: float,
    url: str,
    style: str,
) -> str:
    """Build a complete, cleanly aligned terminal output frame with strict reconciliation."""
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")

    # Column specifications: (Title, Display Width, Alignment)
    # Total widths: 11 + 16 + 8 + 8 + 15 + 6 + 7 + 14 + 6 + 8 = 99
    # Compact width: 99 + 9 = 108
    # Box width: 99 + 10 * 3 + 1 = 130
    cols = [
        ("客户端类型", 11, "left"),
        ("实时并发/容量", 16, "left"),
        ("10分窗口用量", 12, "right"),
        ("排队/上限", 10, "right"),
        ("历史排队", 8, "right"),
        ("准入(直通+排队)", 15, "right"),
        ("拒绝", 6, "right"),
        ("总请求", 7, "right"),
        ("均等 / 最长耗时", 14, "right"),
        ("QPS", 6, "right"),
        ("健康状态", 8, "center"),
    ]

    top_border = "┌─" + "─┬─".join("─" * w for _, w, _ in cols) + "─┐"
    box_table_width = len(top_border)
    inner_w = box_table_width - 4

    compact_divider = " ".join("─" * w for _, w, _ in cols)
    compact_table_width = len(compact_divider)

    lines = []

    if not metrics:
        err_line = f"\033[91m[错误] 无法连接到限流监控端点 ({url}): {ts}\033[0m"
        lines.append("=" * 80)
        lines.append("       CLIProxyAPI 客户端限流 & 并发排队看板 (离线/未响应)       ")
        lines.append("=" * 80)
        lines.append(err_line)
        lines.append("-" * 80)
        lines.append("请确认 ./cli-proxy-api 是否已启动，且端口与 Token 正确。")
        return "\n".join(lines)

    # Compute global aggregates with rigorous reconciliation
    tot_in_flight = 0
    tot_cap = 0
    tot_curr_queue = 0
    tot_hist_queued = 0
    tot_req = 0
    tot_admitted = 0
    tot_fast = 0
    tot_queued_adm = 0
    tot_rej = 0
    tot_auth = 0
    tot_win_reqs = 0
    tot_win_max = 0

    reconciled_rows = {}

    for client_type in sorted(metrics.keys()):
        m = metrics[client_type]
        in_flight = m.get("in_flight_requests", 0)
        capacity = m.get("effective_capacity", 0)
        queue_depth = m.get("queue_depth", 0)
        total = m.get("total_requests", 0)
        admitted = m.get("admitted_requests", 0)
        fast_adm = m.get("fast_admitted_requests", 0)
        queued_adm = m.get("queued_admitted_requests", 0)
        queued = m.get("queued_requests", 0)
        rej_full = m.get("rejected_queue_full", 0)
        rej_to = m.get("rejected_timeout", 0)
        rejected = rej_full + rej_to
        canceled = m.get("canceled_in_queue", 0)
        avg_wait = m.get("avg_wait_ms", 0)
        max_wait = m.get("max_wait_ms", 0)
        auth_count = m.get("active_auth_count", 0)
        win_reqs = m.get("window_requests", 0)
        win_max = m.get("window_max_requests", 0)
        win_dur_s = m.get("window_duration_seconds", 600)
        win_mins = int(round(win_dur_s / 60)) if win_dur_s > 0 else 10
        max_q = m.get("effective_max_queue", 0) or m.get("max_queue_size", 0)

        # Reconcile numbers mathematically:
        # If the plugin didn't separate fast vs queued admitted, derive them:
        if fast_adm == 0 and queued_adm == 0 and queued > 0:
            fast_adm = admitted
            queued_adm = max(0, queued - rej_to - rej_full - canceled - queue_depth)
            admitted = fast_adm + queued_adm
        elif admitted == 0 and (fast_adm > 0 or queued_adm > 0):
            admitted = fast_adm + queued_adm

        reconciled_rows[client_type] = {
            "in_flight": in_flight,
            "capacity": capacity,
            "queue_depth": queue_depth,
            "max_queue": max_q,
            "queued": queued,
            "fast_adm": fast_adm,
            "queued_adm": queued_adm,
            "admitted": admitted,
            "rejected": rejected,
            "total": total,
            "avg_wait": avg_wait,
            "max_wait": max_wait,
            "auth_count": auth_count,
            "win_reqs": win_reqs,
            "win_max": win_max,
            "win_mins": win_mins,
        }

        tot_in_flight += in_flight
        tot_cap += capacity
        tot_curr_queue += queue_depth
        tot_hist_queued += queued
        tot_req += total
        tot_admitted += admitted
        tot_fast += fast_adm
        tot_queued_adm += queued_adm
        tot_rej += rejected
        tot_auth += auth_count
        tot_win_reqs += win_reqs
        tot_win_max += win_max

    # Total QPS
    curr_total_all = tot_req
    prev_total_all = sum(prev_totals.values()) if prev_totals else curr_total_all
    tot_qps = (
        (curr_total_all - prev_total_all) / elapsed
        if elapsed > 0 and prev_totals
        else 0.0
    )

    q_highlight = f"\033[93m{tot_curr_queue}\033[0m" if tot_curr_queue > 0 else "0"
    rej_highlight = f"\033[91m{tot_rej}\033[0m" if tot_rej > 0 else "0"
    pass_rate = (tot_admitted / tot_req * 100) if tot_req > 0 else 100.0

    display_url = url
    if len(display_url) > 42:
        m_port = re.search(r":(\d+)", display_url)
        port_str = f":{m_port.group(1)}" if m_port else ""
        display_url = f"http://127.0.0.1{port_str}/.../metrics"

    win_sum_str = f" │ 10分窗口 {tot_win_reqs}/{tot_win_max}" if tot_win_max > 0 else ""

    if style == "box":
        title = pad(
            "CLIProxyAPI 客户端限流 & 并发排队实时监控看板 (RateLimit Monitor)",
            inner_w,
            "center",
        )
        sub_info = pad(
            f"\033[1;36m[刷新时间]:\033[0m {now_str}   \033[1;36m[快照时间]:\033[0m {ts}   \033[1;36m[监控端点]:\033[0m {display_url}",
            inner_w,
            "left",
        )
        overview = pad(
            f"\033[1;32m[全局汇总]:\033[0m 在途并发 {tot_in_flight}/{tot_cap}{win_sum_str} │ 当前排队 {q_highlight} │ 历史排队 {tot_hist_queued} │ 累计请求 {tot_req} (准入 {tot_admitted} / 拒绝 {rej_highlight}) │ 准入率 {pass_rate:.1f}% │ 总QPS {tot_qps:.1f}",
            inner_w,
            "left",
        )
        formula_box = pad(
            f"\033[1;33m[对账平衡]:\033[0m 总请求 ({tot_req}) = 累计准入 [{tot_admitted} (直通 {tot_fast} + 排队放行 {tot_queued_adm})] + 累计拒绝 ({tot_rej}) + 当前排队中 ({tot_curr_queue})",
            inner_w,
            "left",
        )

        lines.append("┌" + "─" * (inner_w + 2) + "┐")
        lines.append("│ " + title + " │")
        lines.append("├" + "─" * (inner_w + 2) + "┤")
        lines.append("│ " + sub_info + " │")
        lines.append("│ " + overview + " │")
        lines.append("│ " + formula_box + " │")
        lines.append("└" + "─" * (inner_w + 2) + "┘")

        # Table with borders
        header = (
            "│ "
            + " │ ".join(pad(t, w, align) for t, w, align in cols)
            + " │"
        )
        mid_border = "├─" + "─┼─".join("─" * w for _, w, _ in cols) + "─┤"
        bot_border = "└─" + "─┴─".join("─" * w for _, w, _ in cols) + "─┘"

        lines.append(top_border)
        lines.append(header)
        lines.append(mid_border)

        for client_type in sorted(metrics.keys()):
            r = reconciled_rows[client_type]
            in_flight = r["in_flight"]
            capacity = r["capacity"]
            queue_depth = r["queue_depth"]
            queued = r["queued"]
            fast_adm = r["fast_adm"]
            queued_adm = r["queued_adm"]
            admitted = r["admitted"]
            rejected = r["rejected"]
            total = r["total"]
            avg_wait = r["avg_wait"]
            max_wait = r["max_wait"]

            prev_t = prev_totals.get(client_type, total)
            qps = (
                (total - prev_t) / elapsed
                if elapsed > 0 and client_type in prev_totals
                else 0.0
            )

            win_reqs = r["win_reqs"]
            win_max = r["win_max"]
            if win_max > 0:
                if win_reqs >= win_max:
                    win_display = f"\033[91m{win_reqs}/{win_max}\033[0m"
                elif win_reqs >= int(win_max * 0.8):
                    win_display = f"\033[93m{win_reqs}/{win_max}\033[0m"
                else:
                    win_display = f"{win_reqs}/{win_max}"
            else:
                win_display = "--"

            bar_str = render_bar(in_flight, capacity)
            max_q = r.get("max_queue", 0)
            if max_q > 0:
                q_depth_str = (
                    f"\033[93m{queue_depth}\033[0m/{max_q}"
                    if queue_depth > 0
                    else f"0/{max_q}"
                )
            else:
                q_depth_str = (
                    f"\033[93m{queue_depth}\033[0m" if queue_depth > 0 else "0"
                )
            hist_queued_str = str(queued)
            adm_breakdown = f"{admitted} ({fast_adm}+{queued_adm})"
            rej_str = f"\033[91m{rejected}\033[0m" if rejected > 0 else "0"
            wait_str = (
                f"{format_time_short(avg_wait)} / {format_time_short(max_wait)}"
            )

            if queue_depth > 0:
                if in_flight < capacity and win_max > 0 and win_reqs >= win_max:
                    status = "\033[93m[窗口满]\033[0m"
                else:
                    status = "\033[93m[排队中]\033[0m"
            elif rejected > 0 and in_flight >= capacity:
                status = "\033[91m[限流中]\033[0m"
            elif in_flight >= capacity and capacity > 0:
                status = "\033[91m[满负荷]\033[0m"
            elif in_flight > 0:
                status = "\033[92m[运行中]\033[0m"
            else:
                status = "\033[90m[空闲]\033[0m"

            row = [
                pad(client_type, 11, "left"),
                pad(bar_str, 16, "left"),
                pad(win_display, 12, "right"),
                pad(q_depth_str, 10, "right"),
                pad(hist_queued_str, 8, "right"),
                pad(adm_breakdown, 15, "right"),
                pad(rej_str, 6, "right"),
                pad(str(total), 7, "right"),
                pad(wait_str, 14, "right"),
                pad(f"{qps:.1f}", 6, "right"),
                pad(status, 8, "center"),
            ]
            lines.append("│ " + " │ ".join(row) + " │")

        lines.append(bot_border)
        footer_info = "\033[90m字段说明: [10分窗口用量]=近10分钟窗口请求量/上限 | [当前排队]=此刻等待数 | [准入]=总放行数 | 对账公式: 总请求 = 准入 + 拒绝 + 当前排队\033[0m"
        lines.append(footer_info)

    else:
        # Compact style
        lines.append("=" * compact_table_width)
        lines.append(
            pad(
                "CLIProxyAPI 客户端限流 & 并发排队看板 (RateLimit Monitor)",
                compact_table_width,
                "center",
            )
        )
        lines.append("=" * compact_table_width)
        lines.append(
            f"\033[1;36m[监控时间]:\033[0m {now_str}   \033[1;36m[快照时间]:\033[0m {ts}   \033[1;36m[端点]:\033[0m {display_url}"
        )
        lines.append(
            f"\033[1;32m[全局汇总]:\033[0m 在途 {tot_in_flight}/{tot_cap}{win_sum_str} │ 当前排队 {q_highlight} │ 历史排队 {tot_hist_queued} │ 累计 {tot_req} (准入 {tot_admitted} / 拒绝 {rej_highlight}) │ 准入率 {pass_rate:.1f}% │ QPS {tot_qps:.1f}"
        )
        lines.append(
            f"\033[1;33m[对账平衡]:\033[0m 总请求 ({tot_req}) = 准入 [{tot_admitted} (直通 {tot_fast} + 排队放行 {tot_queued_adm})] + 拒绝 ({tot_rej}) + 当前排队中 ({tot_curr_queue})"
        )
        lines.append("─" * compact_table_width)

        sep = " "
        header = sep.join(pad(title, w, align) for title, w, align in cols)
        lines.append(header)
        lines.append(compact_divider)

        for client_type in sorted(metrics.keys()):
            r = reconciled_rows[client_type]
            in_flight = r["in_flight"]
            capacity = r["capacity"]
            queue_depth = r["queue_depth"]
            queued = r["queued"]
            fast_adm = r["fast_adm"]
            queued_adm = r["queued_adm"]
            admitted = r["admitted"]
            rejected = r["rejected"]
            total = r["total"]
            avg_wait = r["avg_wait"]
            max_wait = r["max_wait"]
            win_reqs = r["win_reqs"]
            win_max = r["win_max"]

            prev_t = prev_totals.get(client_type, total)
            qps = (
                (total - prev_t) / elapsed
                if elapsed > 0 and client_type in prev_totals
                else 0.0
            )

            if win_max > 0:
                if win_reqs >= win_max:
                    win_display = f"\033[91m{win_reqs}/{win_max}\033[0m"
                elif win_reqs >= int(win_max * 0.8):
                    win_display = f"\033[93m{win_reqs}/{win_max}\033[0m"
                else:
                    win_display = f"{win_reqs}/{win_max}"
            else:
                win_display = "--"

            bar_str = render_bar(in_flight, capacity)
            max_q = r.get("max_queue", 0)
            if max_q > 0:
                q_depth_str = (
                    f"\033[93m{queue_depth}\033[0m/{max_q}"
                    if queue_depth > 0
                    else f"0/{max_q}"
                )
            else:
                q_depth_str = (
                    f"\033[93m{queue_depth}\033[0m" if queue_depth > 0 else "0"
                )
            hist_queued_str = str(queued)
            adm_breakdown = f"{admitted} ({fast_adm}+{queued_adm})"
            rej_str = f"\033[91m{rejected}\033[0m" if rejected > 0 else "0"
            wait_str = (
                f"{format_time_short(avg_wait)} / {format_time_short(max_wait)}"
            )

            if queue_depth > 0:
                if in_flight < capacity and win_max > 0 and win_reqs >= win_max:
                    status = "\033[93m[窗口满]\033[0m"
                else:
                    status = "\033[93m[排队中]\033[0m"
            elif rejected > 0 and in_flight >= capacity:
                status = "\033[91m[限流中]\033[0m"
            elif in_flight >= capacity and capacity > 0:
                status = "\033[91m[满负荷]\033[0m"
            elif in_flight > 0:
                status = "\033[92m[运行中]\033[0m"
            else:
                status = "\033[90m[空闲]\033[0m"

            row = [
                pad(client_type, 11, "left"),
                pad(bar_str, 16, "left"),
                pad(win_display, 12, "right"),
                pad(q_depth_str, 10, "right"),
                pad(hist_queued_str, 8, "right"),
                pad(adm_breakdown, 15, "right"),
                pad(rej_str, 6, "right"),
                pad(str(total), 7, "right"),
                pad(wait_str, 14, "right"),
                pad(f"{qps:.1f}", 6, "right"),
                pad(status, 8, "center"),
            ]
            lines.append(sep.join(row))

        lines.append("─" * compact_table_width)
        lines.append(
            "\033[90m对账公式: 总请求 = 准入 + 拒绝 + 当前排队 | 按 Ctrl+C 退出看板\033[0m"
        )

    return "\n".join(lines)


def cleanup():
    """Restore terminal cursor and colors on shutdown."""
    sys.stdout.write("\033[?25h\033[0m\n")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description="CLIProxyAPI 客户端限流 & 并发排队实时监控看板"
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8317/v0/resource/plugins/ratelimit/ratelimit/metrics",
        help="限流指标 API 端点",
    )
    parser.add_argument(
        "--token",
        default="admin123456",
        help="Management Bearer 认证 Token (默认为 admin123456)",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=float,
        default=2.0,
        help="看板刷新间隔秒数 (默认: 2.0s)",
    )
    parser.add_argument(
        "--style",
        choices=["auto", "box", "compact"],
        default="auto",
        help="看板样式: auto (自适应), box (全边框), compact (紧凑)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="仅输出一次当前指标快照后退出",
    )

    args = parser.parse_args()

    # Determine layout style (box mode width is 130)
    term_cols = shutil.get_terminal_size((120, 30)).columns
    if args.style == "auto":
        resolved_style = "box" if term_cols >= 132 else "compact"
    else:
        resolved_style = args.style

    atexit.register(cleanup)

    if args.once:
        metrics, ts = fetch_metrics(args.url, args.token)
        frame = build_frame(metrics, ts, {}, 0.0, args.url, resolved_style)
        print(frame)
        return

    prev_totals = {}
    prev_time = time.time()

    sys.stdout.write("\033[?25l\033[2J\033[H")
    sys.stdout.flush()

    try:
        while True:
            metrics, ts = fetch_metrics(args.url, args.token)
            now = time.time()
            elapsed = now - prev_time if prev_time else args.interval
            prev_time = now

            if args.style == "auto":
                curr_cols = shutil.get_terminal_size((120, 30)).columns
                resolved_style = "box" if curr_cols >= 132 else "compact"

            frame_text = build_frame(
                metrics, ts, prev_totals, elapsed, args.url, resolved_style
            )

            if metrics:
                for k, v in metrics.items():
                    prev_totals[k] = v.get("total_requests", 0)

            buf = ["\033[H"]
            for line in frame_text.split("\n"):
                buf.append(line + "\033[K\n")
            buf.append("\033[J")

            sys.stdout.write("".join(buf))
            sys.stdout.flush()

            time.sleep(args.interval)

    except KeyboardInterrupt:
        cleanup()
        print("\n\033[32m[RateLimit Monitor] 监控已平稳退出。\033[0m")


if __name__ == "__main__":
    main()
