import pandas as pd
import os
from datetime import datetime


def export_excel(data, keyword):

    os.makedirs("exports", exist_ok=True)

    filename = f"exports/prospects_{keyword}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    df = pd.DataFrame(data)

    df.to_excel(filename, index=False)

    return filename