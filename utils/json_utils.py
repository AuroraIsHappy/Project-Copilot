import re
import json
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _extract_balanced_json_segments(text: str, opener: str, closer: str) -> list[tuple[int, str]]:
    """Extract balanced JSON-like segments for objects/arrays from free-form text."""
    segments: list[tuple[int, str]] = []
    depth = 0
    start = -1
    in_string = False
    escaped = False

    for idx, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == opener:
            if depth == 0:
                start = idx
            depth += 1
            continue

        if ch == closer and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                segments.append((start, text[start: idx + 1]))
                start = -1

    return segments


def extract_json(text: str):
    """
    简化的JSON提取函数，优先处理纯JSON，
    并兼容模型返回中夹杂说明文本的场景。
    """
    if not text:
        return []
    
    text = text.strip()
    
    # 尝试直接解析
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # 尝试提取代码块
    code_block_match = re.search(r'```(?:json)?\s*([\s\S]*?)```', text)
    if code_block_match:
        try:
            return json.loads(code_block_match.group(1).strip())
        except json.JSONDecodeError:
            pass
    
    # 尝试提取JSON数组
    array_match = re.search(r'(\[[\s\S]*\])', text)
    if array_match:
        try:
            return json.loads(array_match.group(1))
        except json.JSONDecodeError:
            pass

    # 尝试从自由文本中提取平衡的 JSON 对象/数组片段。
    # 这对类似 "Here is the result: { ... }" 的模型输出很关键。
    segments = []
    segments.extend(_extract_balanced_json_segments(text, "{", "}"))
    segments.extend(_extract_balanced_json_segments(text, "[", "]"))
    segments.sort(key=lambda item: item[0])

    for _, segment in segments:
        try:
            return json.loads(segment)
        except json.JSONDecodeError:
            continue
    
    return []



def write_json(file_path: Path, data: Dict[str, Any], indent: int = 2) -> bool:
    """
    将数据写入JSON文件。
    
    Args:
        file_path: 文件路径（Path对象或字符串）
        data: 要写入的数据字典
        indent: JSON缩进空格数，默认2
    
    Returns:
        bool: 写入成功返回True，失败返回False
    """
    try:
        # 确保是Path对象
        file_path = Path(file_path)
        
        # 确保目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入JSON文件
        file_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=indent, default=_json_default),
            encoding="utf-8"
        )
        return True
        
    except (OSError, TypeError, ValueError) as e:
        print(f"写入JSON文件失败 {file_path}: {e}")
        return False



def read_json(file_path: Path, default: Optional[Dict] = None) -> Dict:
    """
    从JSON文件读取数据。
    
    Args:
        file_path: 文件路径（Path对象或字符串）
        default: 文件不存在或读取失败时返回的默认值
    
    Returns:
        Dict: 读取的数据字典，失败返回默认值或空字典
    """
    # 设置默认值
    if default is None:
        default = {}
    
    try:
        file_path = Path(file_path)
        
        # 检查文件是否存在
        if not file_path.exists():
            print(f"JSON文件不存在: {file_path}")
            return default
        
        # 读取并解析JSON
        content = file_path.read_text(encoding="utf-8")
        data = json.loads(content)
        
        # 确保返回的是字典
        if not isinstance(data, dict):
            print(f"JSON文件格式错误：根级别不是字典 {file_path}")
            return default
            
        return data
        
    except json.JSONDecodeError as e:
        print(f"JSON解析失败 {file_path}: {e}")
        return default
    except OSError as e:
        print(f"读取JSON文件失败 {file_path}: {e}")
        return default
    except Exception as e:
        print(f"未知错误 {file_path}: {e}")
        return default