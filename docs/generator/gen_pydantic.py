# libs/contracts/generator/gen_pydantic.py
from __future__ import annotations
import re
import sys
import json
import yaml
import textwrap
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Any, List

ROOT = Path(__file__).resolve().parents[2]  # repo root (adjust if needed)
CONTRACTS_DIR = ROOT / "libs" / "contracts"
OUT_DIR = CONTRACTS_DIR / "generated"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- 基础类型映射（可扩展） ---
PY_TYPES = {
    "string": "str",
    "int": "int",
    "float": "float",
    "bool": "bool",
    "date": "date",
    "datetime": "datetime",
    "decimal": "Decimal",  # 如果你需要
    "enum": None,          # 特殊处理
}

HEADER = """\
# This file is AUTO-GENERATED from YAML contracts. DO NOT EDIT MANUALLY.
from __future__ import annotations
from typing import Optional, List
from pydantic import BaseModel, Field, conint, confloat, constr
from enum import Enum
from datetime import date, datetime
"""

def snake_to_camel(s: str) -> str:
    return "".join(part.capitalize() for part in re.split(r"[_\-\s]+", s))

def load_yaml(fp: Path) -> Dict[str, Any]:
    with fp.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def field_decl(f: Dict[str, Any]) -> str:
    """
    根据字段定义生成：类型注解 + Field(...)
    自动选择受约束类型（constr/conint/confloat）与 Enum。
    """
    name = f["name"]
    ftype = f["type"]
    required = bool(f.get("required", False))
    default = f.get("default", ...)
    allowed = f.get("allowed_values")
    regex = f.get("regex")
    min_v = f.get("min")
    max_v = f.get("max")
    nullable = f.get("nullable", False)
    desc = f.get("description")

    # 生成 Python 类型字符串
    if ftype == "enum":
        enum_name = snake_to_camel(name)
        py_type = enum_name
    elif ftype == "string":
        # 如果有 regex 或者需要长度限制，可用 constr
        if regex:
            py_type = "constr(regex=r%r)" % regex
        else:
            py_type = "str"
    elif ftype == "int":
        if min_v is not None or max_v is not None:
            min_kw = f"ge={min_v}" if min_v is not None else ""
            max_kw = f"le={max_v}" if max_v is not None else ""
            comma = "," if (min_kw and max_kw) else ""
            py_type = f"conint({min_kw}{comma}{max_kw})"
        else:
            py_type = "int"
    elif ftype == "float":
        if min_v is not None or max_v is not None:
            min_kw = f"ge={min_v}" if min_v is not None else ""
            max_kw = f"le={max_v}" if max_v is not None else ""
            comma = "," if (min_kw and max_kw) else ""
            py_type = f"confloat({min_kw}{comma}{max_kw})"
        else:
            py_type = "float"
    else:
        py_type = PY_TYPES.get(ftype, "str")  # 兜底

    # Optional 处理
    ann_type = py_type
    if not required or nullable:
        ann_type = f"Optional[{py_type}]"

    # Field(...)
    field_args = []
    if desc:
        field_args.append(f'description="{desc}"')
    if allowed and ftype != "enum":
        # 非 enum 的 allowed 列表用正则限制可选，也可只做注释
        # 这里以注释形式保留（更稳妥；真正强制可用 custom validator）
        field_args.append(f'json_schema_extra={{"allowed": {allowed}}}')

    # default 值
    default_repr = "..." if required and default is ... else repr(default)

    # 合并
    field_str = f"{name}: {ann_type} = Field({default_repr}"
    if field_args:
        field_str += ", " + ", ".join(field_args)
    field_str += ")"
    return field_str

def build_enum_block(field: Dict[str, Any]) -> str:
    name = field["name"]
    values: List[str] = field.get("allowed_values", [])
    enum_name = snake_to_camel(name)
    lines = [f"class {enum_name}(Enum):"]
    if not values:
        lines.append("    pass")
        return "\n".join(lines)
    for v in values:
        member = re.sub(r"[^A-Za-z0-9_]", "_", v.upper())
        lines.append(f'    {member} = "{v}"')
    return "\n".join(lines)

def generate_one(yaml_path: Path) -> Path:
    spec = load_yaml(yaml_path)
    model_name = snake_to_camel(spec["name"])
    fields = spec.get("schema", [])

    enums: List[str] = []
    for f in fields:
        if f["type"] == "enum":
            enums.append(build_enum_block(f))

    # 字段声明
    field_lines = [field_decl(f) for f in fields]

    out_code = [HEADER]
    if enums:
        out_code.append("\n\n".join(enums))
        out_code.append("")

    out_code.append(f"class {model_name}(BaseModel):")
    if not field_lines:
        out_code.append("    pass")
    else:
        out_code.extend("    " + ln for ln in field_lines)

    out_code.append("")  # newline
    out_path = OUT_DIR / f"{spec['name']}.py"
    out_path.write_text("\n".join(out_code), encoding="utf-8")
    return out_path

def write_init_py(paths: List[Path]) -> None:
    exports = []
    for p in paths:
        mod = p.stem
        content = p.read_text(encoding="utf-8")
        classes = re.findall(r"class\s+([A-Za-z0-9_]+)\(BaseModel\):", content)
        enums = re.findall(r"class\s+([A-Za-z0-9_]+)\(Enum\):", content)
        # from .prices_daily import PricesDaily, JobStatus
        names = ", ".join(classes + enums)
        exports.append(f"from .{mod} import {names}")
    exports.append("")
    (OUT_DIR / "__init__.py").write_text("\n".join(exports), encoding="utf-8")

def main():
    yaml_files = sorted(CONTRACTS_DIR.glob("*.yaml"))
    if not yaml_files:
        print("No YAML contracts found.", file=sys.stderr)
        sys.exit(1)
    out_paths = [generate_one(y) for y in yaml_files]
    write_init_py(out_paths)
    print(f"Generated {len(out_paths)} models into {OUT_DIR}")

if __name__ == "__main__":
    main()
