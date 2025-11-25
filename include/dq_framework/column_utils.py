# column_utils.py
import re
import pandas as pd

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to:
      - lowercase
      - strip whitespace
      - replace spaces and separators with underscores
      - keep only [a-z0-9_]
      - collapse multiple underscores
    """
    def _norm(col: str) -> str:
        col = str(col).strip().lower()
        # replace common separators with underscore
        col = re.sub(r"[ \t\r\n/\\\-]+", "_", col)
        # remove anything that's not alnum or underscore
        col = re.sub(r"[^a-z0-9_]", "", col)
        # collapse multiple underscores
        col = re.sub(r"_+", "_", col)
        # trim underscores from ends
        col = col.strip("_")
        return col

    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]
    return df
