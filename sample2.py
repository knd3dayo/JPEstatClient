from dotenv import load_dotenv
from jpestat_client import JPEStatClient, JPEStatData, JPEStatListData, init_env
if __name__ == "__main__":
    import os

    init_env()
    app_id = os.getenv("JPESTAT_APP_ID", "")
    if not app_id:
        raise ValueError("JPESTAT_APP_ID is not set in the environment variables.")
    client = JPEStatClient(app_id=app_id, lang="J")
    params = {
        "statsDataId": "0004014855",
    }
    # 
    # 統計表情報を取得
    data: JPEStatData = client.get_stat_data_object(params=params)
    df = data.get_value_df()
    df.to_csv("stat_02.csv", index=False, encoding="utf-8")
