import os
from jpestat_client import JPEStatClient, JPEStatData, JPEStatListData, init_env
if __name__ == "__main__":

    init_env()
    app_id = os.getenv("JPESTAT_APP_ID", "")
    if not app_id:
        raise ValueError("JPESTAT_APP_ID is not set in the environment variables.")
    client = JPEStatClient(app_id=app_id, lang="J")
    params = {
        "surveyYears": "2020",
    }
    # 
    # 統計表情報を取得
    data_list_object: JPEStatListData = client.get_stat_list_object(params=params)
    df = data_list_object.get_df()
    df.to_csv("stat_list.csv", index=False, encoding="utf-8")
