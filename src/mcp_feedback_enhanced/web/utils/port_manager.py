"""
端口管理工具模組

提供增強的端口管理功能，包括：
- 智能端口查找
- 進程檢測和清理
- 端口衝突解決
- 多實例端口注冊表（跨進程協調）
"""

import json
import os
import socket
import sys
import time
from contextlib import contextmanager
from typing import Any

import psutil

from ...debug import debug_log


class PortRegistry:
    """端口注冊表 - 跨進程端口分配協調

    使用文件鎖實現原子性端口分配，防止多個 MCP 實例之間的端口衝突。
    注冊表文件存儲在 ~/.mcp-feedback-enhanced/port-registry.json。
    """

    REGISTRY_DIR = os.path.expanduser("~/.mcp-feedback-enhanced")
    REGISTRY_FILE = "port-registry.json"
    LOCK_FILE = "port-registry.lock"

    @classmethod
    def _get_registry_path(cls) -> str:
        return os.path.join(cls.REGISTRY_DIR, cls.REGISTRY_FILE)

    @classmethod
    def _get_lock_path(cls) -> str:
        return os.path.join(cls.REGISTRY_DIR, cls.LOCK_FILE)

    @classmethod
    def _ensure_dir(cls) -> None:
        os.makedirs(cls.REGISTRY_DIR, exist_ok=True)

    @classmethod
    @contextmanager
    def _file_lock(cls):
        """跨平台文件鎖上下文管理器"""
        cls._ensure_dir()
        lock_path = cls._get_lock_path()
        lock_fd = open(lock_path, "w")  # noqa: SIM115
        try:
            if sys.platform == "win32":
                import msvcrt

                lock_fd.write(" ")
                lock_fd.flush()
                lock_fd.seek(0)
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                if sys.platform == "win32":
                    import msvcrt

                    lock_fd.seek(0)
                    msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except Exception:
                pass
            lock_fd.close()

    @classmethod
    def _read_registry(cls) -> dict:
        """讀取注冊表文件，損壞時重建"""
        path = cls._get_registry_path()
        if not os.path.exists(path):
            return {"instances": {}}
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "instances" not in data:
                return {"instances": {}}
            return data
        except (json.JSONDecodeError, OSError):
            return {"instances": {}}

    @classmethod
    def _write_registry(cls, data: dict) -> None:
        cls._ensure_dir()
        path = cls._get_registry_path()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def _is_process_alive(cls, pid: int, start_time: float) -> bool:
        """檢查進程是否存活且匹配預期的啟動時間（防止 PID 重用）"""
        if not psutil.pid_exists(pid):
            return False
        try:
            proc = psutil.Process(pid)
            if abs(proc.create_time() - start_time) > 2.0:
                return False
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    @classmethod
    def _cleanup_stale_entries(cls, registry: dict) -> int:
        """清理過期注冊條目（PID 已不存活的）。必須在文件鎖內調用。"""
        stale_ports = []
        for port_str, info in registry.get("instances", {}).items():
            pid = info.get("pid")
            start_time = info.get("start_time", 0)
            if pid is None or not cls._is_process_alive(pid, start_time):
                stale_ports.append(port_str)
        for port_str in stale_ports:
            del registry["instances"][port_str]
            debug_log(f"清理過期端口注冊: port={port_str}")
        return len(stale_ports)

    @staticmethod
    def _is_port_available(host: str, port: int) -> bool:
        """輕量級端口可用性檢查"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind((host, port))
                return True
        except OSError:
            return False

    @classmethod
    def allocate_port(
        cls,
        base_port: int = 8765,
        host: str = "127.0.0.1",
        max_attempts: int = 100,
    ) -> int:
        """原子性端口分配：在文件鎖保護下查找、檢查、注冊端口

        從 base_port 開始遞增查找，跳過已注冊和不可用的端口。
        整個操作在文件鎖內完成，消除 TOCTOU 競態條件。

        Args:
            base_port: 基準端口號
            host: 綁定主機地址
            max_attempts: 最大嘗試次數

        Returns:
            分配到的可用端口號

        Raises:
            RuntimeError: 找不到可用端口
        """
        with cls._file_lock():
            registry = cls._read_registry()
            cleaned = cls._cleanup_stale_entries(registry)
            if cleaned > 0:
                debug_log(f"端口注冊表清理了 {cleaned} 個過期條目")

            registered_ports = {
                int(p) for p in registry.get("instances", {})
            }

            for i in range(max_attempts):
                port = base_port + i
                if port > 65535:
                    break
                if port in registered_ports:
                    continue
                if cls._is_port_available(host, port):
                    registry["instances"][str(port)] = {
                        "pid": os.getpid(),
                        "start_time": time.time(),
                    }
                    cls._write_registry(registry)
                    debug_log(
                        f"端口注冊表分配端口: {port} (PID: {os.getpid()})"
                    )
                    return port

            raise RuntimeError(
                f"無法在 {base_port}-{base_port + max_attempts} 範圍內找到可用端口。"
                f"活躍實例: {len(registered_ports)} 個"
            )

    @classmethod
    def release_port(cls, port: int) -> None:
        """釋放已注冊的端口（僅釋放當前進程注冊的）"""
        try:
            with cls._file_lock():
                registry = cls._read_registry()
                port_str = str(port)
                info = registry.get("instances", {}).get(port_str)
                if info and info.get("pid") == os.getpid():
                    del registry["instances"][port_str]
                    cls._write_registry(registry)
                    debug_log(f"端口注冊表釋放端口: {port}")
        except Exception as e:
            debug_log(f"釋放端口 {port} 注冊時發生錯誤: {e}")

    @classmethod
    def is_sibling_mcp_process(cls, port: int) -> bool:
        """檢查端口是否由活躍的兄弟 MCP 實例占用"""
        try:
            with cls._file_lock():
                registry = cls._read_registry()
                port_str = str(port)
                info = registry.get("instances", {}).get(port_str)
                if not info:
                    return False
                pid = info.get("pid")
                start_time = info.get("start_time", 0)
                if pid is None:
                    return False
                return pid != os.getpid() and cls._is_process_alive(
                    pid, start_time
                )
        except Exception:
            return False

    @classmethod
    def get_active_instances(cls) -> dict:
        """獲取所有活躍的注冊實例"""
        try:
            with cls._file_lock():
                registry = cls._read_registry()
                cls._cleanup_stale_entries(registry)
                cls._write_registry(registry)
                return dict(registry.get("instances", {}))
        except Exception as e:
            debug_log(f"獲取活躍實例列表時發生錯誤: {e}")
            return {}


class PortManager:
    """端口管理器 - 提供增強的端口管理功能"""

    @staticmethod
    def find_process_using_port(port: int) -> dict[str, Any] | None:
        """
        查找占用指定端口的進程

        Args:
            port: 要檢查的端口號

        Returns:
            Dict[str, Any]: 進程信息字典，包含 pid, name, cmdline 等
            None: 如果沒有進程占用該端口
        """
        try:
            for conn in psutil.net_connections(kind="inet"):
                if conn.laddr.port == port and conn.status == psutil.CONN_LISTEN:
                    try:
                        process = psutil.Process(conn.pid)
                        return {
                            "pid": conn.pid,
                            "name": process.name(),
                            "cmdline": " ".join(process.cmdline()),
                            "create_time": process.create_time(),
                            "status": process.status(),
                        }
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # 進程可能已經結束或無權限訪問
                        continue
        except Exception as e:
            debug_log(f"查找端口 {port} 占用進程時發生錯誤: {e}")

        return None

    @staticmethod
    def kill_process_on_port(port: int, force: bool = False) -> bool:
        """
        終止占用指定端口的進程

        Args:
            port: 要清理的端口號
            force: 是否強制終止進程

        Returns:
            bool: 是否成功終止進程
        """
        process_info = PortManager.find_process_using_port(port)
        if not process_info:
            debug_log(f"端口 {port} 沒有被任何進程占用")
            return True

        try:
            pid = process_info["pid"]
            process = psutil.Process(pid)
            process_name = process_info["name"]

            debug_log(f"發現進程 {process_name} (PID: {pid}) 占用端口 {port}")

            # 檢查是否是自己的進程（避免誤殺）
            if "mcp-feedback-enhanced" in process_info["cmdline"].lower():
                debug_log("檢測到 MCP Feedback Enhanced 相關進程，嘗試優雅終止")

            if force:
                debug_log(f"強制終止進程 {process_name} (PID: {pid})")
                process.kill()
            else:
                debug_log(f"優雅終止進程 {process_name} (PID: {pid})")
                process.terminate()

            # 等待進程結束
            try:
                process.wait(timeout=5)
                debug_log(f"成功終止進程 {process_name} (PID: {pid})")
                return True
            except psutil.TimeoutExpired:
                if not force:
                    debug_log(f"優雅終止超時，強制終止進程 {process_name} (PID: {pid})")
                    process.kill()
                    process.wait(timeout=3)
                    return True
                debug_log(f"強制終止進程 {process_name} (PID: {pid}) 失敗")
                return False

        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            debug_log(f"無法終止進程 (PID: {process_info['pid']}): {e}")
            return False
        except Exception as e:
            debug_log(f"終止端口 {port} 占用進程時發生錯誤: {e}")
            return False

    @staticmethod
    def is_port_available(host: str, port: int) -> bool:
        """
        檢查端口是否可用

        Args:
            host: 主機地址
            port: 端口號

        Returns:
            bool: 端口是否可用
        """
        try:
            # 首先嘗試不使用 SO_REUSEADDR 來檢測端口
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind((host, port))
                return True
        except OSError:
            # 如果綁定失敗，再檢查是否真的有進程在監聽
            # 使用 psutil 檢查是否有進程在監聽該端口
            try:
                import psutil

                for conn in psutil.net_connections(kind="inet"):
                    if (
                        conn.laddr.port == port
                        and conn.laddr.ip in [host, "0.0.0.0", "::"]
                        and conn.status == psutil.CONN_LISTEN
                    ):
                        return False
                # 沒有找到監聽的進程，可能是臨時占用，認為可用
                return True
            except Exception:
                # 如果 psutil 檢查失敗，保守地認為端口不可用
                return False

    @staticmethod
    def find_free_port_enhanced(
        preferred_port: int = 8765,
        auto_cleanup: bool = True,
        host: str = "127.0.0.1",
        max_attempts: int = 100,
    ) -> int:
        """
        增強的端口查找功能（支持多實例協調）

        優先使用 PortRegistry 進行原子性端口分配，消除多實例間的競態條件。
        當注冊表不可用時，回退到傳統的端口查找邏輯。

        Args:
            preferred_port: 偏好端口號
            auto_cleanup: 是否自動清理占用端口的進程
            host: 主機地址
            max_attempts: 最大嘗試次數

        Returns:
            int: 可用的端口號

        Raises:
            RuntimeError: 如果找不到可用端口
        """
        # 優先使用注冊表進行原子性端口分配
        try:
            port = PortRegistry.allocate_port(
                base_port=preferred_port, host=host, max_attempts=max_attempts
            )
            return port
        except Exception as e:
            debug_log(f"端口注冊表分配失敗，回退到傳統方式: {e}")

        # === 回退邏輯：傳統端口查找（注冊表不可用時） ===

        if PortManager.is_port_available(host, preferred_port):
            debug_log(f"偏好端口 {preferred_port} 可用")
            return preferred_port

        if auto_cleanup:
            debug_log(f"偏好端口 {preferred_port} 被占用，嘗試清理占用進程")
            process_info = PortManager.find_process_using_port(preferred_port)

            if process_info:
                debug_log(
                    f"端口 {preferred_port} 被進程 {process_info['name']} (PID: {process_info['pid']}) 占用"
                )

                # 保護活躍的兄弟 MCP 實例
                if PortRegistry.is_sibling_mcp_process(preferred_port):
                    debug_log(
                        f"端口 {preferred_port} 由活躍的兄弟 MCP 實例占用，跳過清理"
                    )
                elif PortManager._should_cleanup_process(process_info):
                    if PortManager.kill_process_on_port(preferred_port):
                        time.sleep(1)
                        if PortManager.is_port_available(host, preferred_port):
                            debug_log(f"成功清理端口 {preferred_port}，現在可用")
                            return preferred_port

        debug_log(f"偏好端口 {preferred_port} 不可用，尋找其他可用端口")

        for i in range(max_attempts):
            port = preferred_port + i + 1
            if PortManager.is_port_available(host, port):
                debug_log(f"找到可用端口: {port}")
                return port

        for i in range(1, min(preferred_port - 1024, max_attempts)):
            port = preferred_port - i
            if port < 1024:
                break
            if PortManager.is_port_available(host, port):
                debug_log(f"找到可用端口: {port}")
                return port

        raise RuntimeError(
            f"無法在 {preferred_port}±{max_attempts} 範圍內找到可用端口。"
            f"請檢查是否有過多進程占用端口，或手動指定其他端口。"
        )

    @staticmethod
    def _should_cleanup_process(process_info: dict[str, Any]) -> bool:
        """
        判斷是否應該清理指定進程

        多實例場景下，絕不清理活躍的兄弟 MCP 實例。
        呼叫端應先透過 PortRegistry.is_sibling_mcp_process() 排除兄弟進程。

        Args:
            process_info: 進程信息字典

        Returns:
            bool: 是否應該清理該進程
        """
        cmdline = process_info.get("cmdline", "").lower()
        process_name = process_info.get("name", "").lower()
        pid = process_info.get("pid")

        if pid == os.getpid():
            debug_log("跳過清理：目標進程是當前進程自身")
            return False

        is_mcp_process = any(
            keyword in cmdline
            for keyword in ["mcp-feedback-enhanced", "mcp_feedback_enhanced"]
        )
        is_related_python = "python" in process_name and any(
            keyword in cmdline for keyword in ["uvicorn", "fastapi"]
        )

        if is_mcp_process or is_related_python:
            # 額外安全檢查：如果此 PID 在注冊表中，說明是活躍兄弟
            try:
                active = PortRegistry.get_active_instances()
                for _port_str, info in active.items():
                    if info.get("pid") == pid:
                        debug_log(
                            f"進程 {process_info['name']} (PID: {pid}) "
                            f"是注冊表中的活躍 MCP 實例，跳過清理"
                        )
                        return False
            except Exception:
                pass
            return True

        debug_log(
            f"進程 {process_info['name']} (PID: {process_info['pid']}) 不是 MCP 相關進程，跳過自動清理"
        )
        return False

    @staticmethod
    def get_port_status(port: int, host: str = "127.0.0.1") -> dict[str, Any]:
        """
        獲取端口狀態信息

        Args:
            port: 端口號
            host: 主機地址

        Returns:
            Dict[str, Any]: 端口狀態信息
        """
        status = {
            "port": port,
            "host": host,
            "available": False,
            "process": None,
            "error": None,
        }

        try:
            # 檢查端口是否可用
            status["available"] = PortManager.is_port_available(host, port)

            # 如果不可用，查找占用進程
            if not status["available"]:
                status["process"] = PortManager.find_process_using_port(port)

        except Exception as e:
            status["error"] = str(e)
            debug_log(f"獲取端口 {port} 狀態時發生錯誤: {e}")

        return status

    @staticmethod
    def list_listening_ports(
        start_port: int = 8000, end_port: int = 9000
    ) -> list[dict[str, Any]]:
        """
        列出指定範圍內正在監聽的端口

        Args:
            start_port: 起始端口
            end_port: 結束端口

        Returns:
            List[Dict[str, Any]]: 監聽端口列表
        """
        listening_ports = []

        try:
            for conn in psutil.net_connections(kind="inet"):
                if (
                    conn.status == psutil.CONN_LISTEN
                    and start_port <= conn.laddr.port <= end_port
                ):
                    try:
                        process = psutil.Process(conn.pid)
                        port_info = {
                            "port": conn.laddr.port,
                            "host": conn.laddr.ip,
                            "pid": conn.pid,
                            "process_name": process.name(),
                            "cmdline": " ".join(process.cmdline()),
                        }
                        listening_ports.append(port_info)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

        except Exception as e:
            debug_log(f"列出監聽端口時發生錯誤: {e}")

        return listening_ports
