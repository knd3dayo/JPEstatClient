import requests # type: ignore[import]
import json
import pandas as pd # type: ignore[import]

'''
日本の政府統計APIを利用するためのクライアントクラス
https://www.e-stat.go.jp/api/api_info/e-stat_api.html
免責事項
「このサービスは、政府統計総合窓口(e-Stat)のAPI機能を使用していますが、サービスの内容は国によって保証されたものではありません。」
https://www.e-stat.go.jp/api/api/api/index.php/api-info/credit
'''

class JPEStatData:
    """
    日本の政府統計APIのデータクラス
    """
    def __init__(self, stat_data: dict ={}):
        self.stat_data = stat_data

    # 統計データからDataFrameを返す
    def get_df(self) -> pd.DataFrame:
        """
        3.4. 統計データ取得
        """
        # DataFrameに変換
        df = pd.json_normalize(self.stat_data.get("GET_STATS_DATA",{}).get("STATISTICAL_DATA",{}).get("TABLE_INF",[]))   
        return df
    
    # 統計データのdictからCLASS_INFを取得してDataFrameを返す
    def __get_stat_data_class_df(self) -> pd.DataFrame:
        """
        統計データからCLASS_OBJを取得
        """
        # DataFrameに変換
        df = pd.json_normalize(self.stat_data.get("GET_STATS_DATA",{}).get("STATISTICAL_DATA",{}).get("CLASS_INF",{}).get("CLASS_OBJ",[]))   
        return df
    
    # 統計データのdictからVLALUEを取得してDataFrameを返す
    def __get_stat_data_value_df(self) -> pd.DataFrame:
        """
        統計データからVALUEを取得
        """
        # DataFrameに変換
        df = pd.json_normalize(self.stat_data.get("GET_STATS_DATA",{}).get("STATISTICAL_DATA",{}).get("DATA_INF",{}).get("VALUE",[]))   
        return df
    
    # 統計データのdictからCLASS_INFを取得してDataFrameを返す

    # 統計データのdictからCLASS_OBJとVALUEを取得して結合したDataFrameを返す
    def get_values_df(self, params: dict ={}) -> pd.DataFrame:
        """
        統計データからCLASS_OBJとVALUEを取得して結合
        「@ + CLASS_OBJの@id列の値」がVALUEの各列名に対応する
        VAULEの列名をCLASS_OBJの@name列の値に置き換える
        @unitは「単位」に置き換える
        $は値に置き換える
        """
        # CLASS_OBJを取得
        class_df = self.__get_stat_data_class_df()
        # VALUEを取得
        value_df = self.__get_stat_data_value_df()
        # CLASS_OBJの@id列をVALUEの各列名に対応させる
        # class_dfの@idをキーに@nameへのマッピングを作成
        # value_dfの列名を置き換える
        class_mapping = { "@" + row["@id"]: row["@name"] for _, row in class_df.iterrows()}
        # @unitを「単位」に置き換える
        class_mapping["@unit"] = "単位"
        # $は値に置き換える
        class_mapping["$"] = "値"
        # value_dfの列名を置き換える
        value_df.rename(columns=class_mapping, inplace=True)
        
        return value_df


class JPEStatClient:
    def __init__(self, app_id: str, lang: str = "J"):
        """"
        "3.1. 全API共通
        パラメータ名	意味	必須	設定内容・設定可能値
        appId	アプリケーションID	〇	取得したアプリケーションIDを指定して下さい。
        lang	言語	－	取得するデータの言語を 以下のいずれかを指定して下さい。
        ・J：日本語 (省略値)
        ・E：英語"
        """
        self.app_id = app_id
        self.lang = lang

    def get_stat_list(self, params: dict ={}) -> dict:
        """
        3.2. 統計表情報取得
        パラメータ名	意味	必須	設定内容・設定可能値
        surveyYears	調査年月	－	以下のいずれかの形式で指定して下さい。
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymm-yyyymm：範囲検索
        openYears	公開年月	－	調査年月と同様です。
        statsField	統計分野	－	以下のいずれかの形式で指定して下さい。
        ・数値2桁：統計大分類で検索
        ・数値4桁：統計小分類で検索
        statsCode	政府統計コード	－	以下のいずれかの形式で指定して下さい。
        ・数値5桁：作成機関で検索
        ・数値8桁：政府統計コードで検索
        searchWord	検索キーワード	－	任意の文字列
        表題やメタ情報等に含まれている文字列を検索します。
        AND、OR 又は NOT を指定して複数ワードでの検索が可能です。 (東京 AND 人口、東京 OR 大阪 等)
        searchKind	検索データ種別	－	検索するデータの種別を指定して下さい。
        ・1：統計情報(省略値)
        ・2：小地域・地域メッシュ
        collectArea
        ※1	集計地域区分	－	検索するデータの集計地域区分を指定して下さい。
        ・1：全国
        ・2：都道府県
        ・3：市区町村
        explanationGetFlg	解説情報有無	－	統計表及び、提供統計、提供分類の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        statsNameList	統計調査名指定	－	統計表情報でなく、統計調査名の一覧を取得する場合に指定して下さい。
        ・Y：統計調査名一覧
        統計調査名一覧を出力します。
        statsNameListパラメータを省略した場合、又はY以外の値を設定した場合は統計表情報を出力します。
        startPosition	データ取得開始位置	－	データの取得開始位置（1から始まる行番号）を指定して下さい。省略時は先頭から取得します。
        統計データを複数回に分けて取得する場合等、継続データを取得する開始位置を指定するために指定します。
        前回受信したデータの<NEXT_KEY>タグの値を指定します。
        limit	データ取得件数	－	データの取得行数を指定して下さい。省略時は10万件です。
        データ件数が指定したlimit値より少ない場合、全件を取得します。データ件数が指定したlimit値より多い場合（継続データが存在する）は、受信したデータの<NEXT_KEY>タグに継続データの開始行が設定されます。
        updatedDate	更新日付	－	更新日付を指定します。指定された期間で更新された統計表の情報）を提供します。以下のいずれかの形式で指定して下さい。
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymmdd：単日検索
        ・yyyymmdd-yyyymmdd：範囲検索
        callback	コールバック関数	△	JSONP形式のデータ呼出の場合は必須パラメータです。
        コールバックされる関数名を指定して下さい。
        """

        url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsList"
        params["appId"] = self.app_id
        params["lang"] = self.lang
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.raise_for_status())
        
    def get_meta_info(self, params: dict ={}) -> dict:
        """
        3.2. メタ情報取得
        パラメータ名	意味	必須	設定内容・設定可能値
        statsDataId	統計表ID	〇	「統計表情報取得」で得られる統計表IDです。
        explanationGetFlg	解説情報有無	－	統計表及び、提供統計、提供分類、各事項の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        callback	コールバック関数	△	JSONP形式のデータ呼出の場合は必須パラメータです。
        コールバックされる関数名を指定して下さい。
        省略時は指定しません。 
        """
        
        url = "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo"
        params["appId"] = self.app_id
        params["lang"] = self.lang
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.raise_for_status())


    def get_catalog(self, params: dict ={}) -> dict:
        """
        3.7. データカタログ情報取得
        パラメータ名	意味	必須	設定内容・設定可能値
        surveyYears	調査年月	－	以下のいずれかの形式で指定して下さい。
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymm-yyyymm：範囲検索
        openYears	公開年月	－	調査年月と同様です。
        statsField	統計分野	－	以下のいずれかの形式で指定して下さい。
        ・数値2桁：統計大分類で検索
        ・数値4桁：統計小分類で検索
        statsCode	政府統計コード	－	以下のいずれかの形式で指定して下さい。
        ・数値5桁：作成機関で検索
        ・数値8桁：政府統計コードで検索
        searchWord	検索キーワード	－	任意の文字列
        表題やメタ情報等に含まれている文字列を検索します。
        AND 、OR 又は NOT を指定して複数ワードでの検索が可能です。 (東京 AND 人口、東京 OR 大阪 等)
        collectArea	集計地域区分	－	検索するデータの集計地域区分を指定して下さい。
        ・1：全国
        ・2：都道府県
        ・3：市区町村
        explanationGetFlg	解説情報有無	－	統計表及び、提供統計、提供分類の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        dataType	検索データ形式	－	以下の値を指定して下さい。
        ・XLS：EXCELファイル
        ・CSV：CSVファイル
        ・PDF：PDFファイル
        ・XML：XMLファイル
        ・XLS_REP：EXCELファイル（閲覧用）
        ・DB：統計データベース
        カンマ区切りで複数指定可能です。
        省略時はすべてを指定した場合と同じです。
        startPosition	データ取得開始位置	－	データの取得開始位置（1から始まる番号）を指定して下さい。省略時は先頭から取得します。
        統計表情報を複数回に分けて取得する場合等、継続データを取得する開始位置（データセット）を指定するために指定します。
        前回受信したデータの<NEXT_KEY>タグの値を指定します。
        catalogId	カタログID	－	検索するカタログIDを指定してください。
        resourceId	カタログリソースID	－	検索するカタログリソースIDを指定してください。
        limit	データ取得件数	－	データの取得データセット数を指定して下さい。省略時は100データセットです。
        データセット数が指定したlimit値より少ない場合、全件を取得します。データセット数が指定したlimit値より多い場合（継続データが存在する）は、受信したデータの<NEXT_KEY>タグに継続データの開始位置が設定されます。
        updatedDate	更新日付	－	更新日付を指定します。指定された期間で更新されたデータセットの情報を提供します。以下のいずれかの形式で指定して下さい。
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymmdd：単日検索
        ・yyyymmdd-yyyymmdd：範囲検索
        callback	コールバック関数	△	JSONP形式のデータ呼出の場合は必須パラメータです。
        コールバックされる関数名を指定して下さい。
        省略時は指定しません。
        """

        url = "https://api.e-stat.go.jp/rest/3.0/app/json/getDataCatalog"
        params["appId"] = self.app_id
        params["lang"] = self.lang
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.raise_for_status())

    def get_stat_data_json(self, params: dict ={}) -> dict:
        """
        3.4. 統計データ取得
        パラメータ名	意味	必須	設定内容・設定可能値
        dataSetId	データセットID ※1,2	△	「データセット登録」で登録したデータセットID です。
        statsDataId	統計表ID ※1	△	「統計表情報取得」で得られる統計表IDです。
        lvTab	絞り込み条件 ※2	表章事項	階層レベル	－	以下のいずれかの形式で指定して下さい。
        (Xは「メタ情報取得」で得られる各メタ情報の階層レベル)
        ・X：指定階層レベルのみで絞り込み
        ・X-X：指定階層レベルの範囲で絞り込み
        ・-X：階層レベル1 から指定階層レベルの範囲で絞り込み
        ・X-：指定階層レベルから階層レベル 9 の範囲で絞り込み
        cdTab	単一コード※3	－	特定の項目コードでの絞り込み
        「メタ情報取得」で得られる各メタ情報の項目コードを指定して下さい。
        コードはカンマ区切りで100個まで指定可能です。
        cdTabFrom	コード From※3	－	項目コードの範囲で絞り込み
        絞り込む範囲の開始位置の項目コードを指定して下さい。
        cdTabTo	コード To※3	－	項目コードの範囲で絞り込み
        絞り込む範囲の終了位置の項目コードを指定して下さい。
        lvTime	時間軸事項	階層レベル	－	表章事項の階層レベルと同様です。
        cdTime	単一コード	－	表章事項の単一コードと同様です。
        cdTimeFrom	コード From	－	表章事項のコード Fromと同様です。
        cdTimeTo	コード To	－	表章事項のコード Toと同様です。
        lvArea	地域事項	階層レベル	－	表章事項の階層レベルと同様です。
        cdArea	単一コード	－	表章事項の単一コードと同様です。
        cdAreaFrom	コード From	－	表章事項のコード Fromと同様です。
        cdAreaTo	コード To	－	表章事項のコード Toと同様です。
        lvCat01	分類事項01	階層レベル	－	表章事項の階層レベルと同様です。
        cdCat01	単一コード	－	表章事項の単一コードと同様です。
        cdCat01From	コード From	－	表章事項のコード Fromと同様です。
        cdCat01To	コード To	－	表章事項のコード Toと同様です。
        ・・・	分類事項02 ～ 15	－	分類事項01と同様です。
        startPosition	データ取得開始位置	－	データの取得開始位置（1から始まる行番号）を指定して下さい。
        省略時は先頭から取得します。
        統計データを複数回に分けて取得する場合等、継続データを取得する開始位置を指定するために指定します。
        前回受信したデータのタグの値を指定します。
        limit	データ取得件数	－	データの取得行数を指定して下さい。省略時は10万件です。
        データ件数が指定したlimit値より少ない場合、全件を取得します。データ件数が指定したlimit値より多い場合（継続データが存在する）は、受信したデータのタグに継続データの開始行が設定されます。
        metaGetFlg	メタ情報有無	－	統計データと一緒にメタ情報を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        CSV形式のデータ呼び出しの場合、本パラメータは無効（N：取得しない）です。
        cntGetFlg	件数取得フラグ	－	指定した場合、件数のみ取得できます。metaGetFlg=Yの場合は、メタ情報も同時に返却されます。
        ・Y：件数のみ取得する。統計データは取得しない。
        ・N：件数及び統計データを取得する。(省略値)
        CSV形式のデータ呼び出しの場合、本パラメータは無効（N：件数及び統計データを取得する）です。
        explanationGetFlg	解説情報有無	－	統計表及び、提供統計、提供分類、各事項の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        annotationGetFlg	注釈情報有無	－	数値データの注釈を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        replaceSpChar	特殊文字の置換	－	特殊文字を置換するか否かを設定します。
        ・置換しない：0（デフォルト）
        ・0（ゼロ）に置換する：1
        ・ NULL（長さ0の文字列、空文字)に置換する：2
        ・ NA（文字列）に置換する：3
        callback	コールバック関数	△	JSONP形式のデータ呼出の場合は必須パラメータです。
        コールバックされる関数名を指定して下さい。
        sectionHeaderFlg	セクションヘッダフラグ	－	CSV形式のデータ呼び出しの場合に有効なパラメータです。
        ・1：セクションヘッダを出力する (省略値)
        ・2：セクションヘッダを取得しない
        """
        url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
        params["appId"] = self.app_id
        params["lang"] = self.lang
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.raise_for_status())

    # 統計データを取得してJPEStatDataクラスを返す
    def get_stat_data_object(self, params: dict ={}) -> JPEStatData:
        """
        3.4. 統計データ取得
        """
        stat_data = self.get_stat_data_json(params=params)
        # JPEStatDataクラスに変換
        return JPEStatData(stat_data=stat_data)
    
    # 統計表情報を取得してDataFrameを返す
    def get_stat_list_df(self, params: dict ={}) -> pd.DataFrame:
        """
        3.2. 統計表情報取得
        """
        stat_list = self.get_stat_list(params=params)
        # DataFrameに変換
        df = pd.json_normalize(stat_list.get("GET_STATS_LIST",{}).get("DATALIST_INF",{}).get("TABLE_INF",[]))   
        return df
    # メタ情報を取得してDataFrameを返す
    def get_meta_info_df(self, params: dict ={}) -> pd.DataFrame:
        """
        3.3. メタ情報取得
        """
        meta_info = self.get_meta_info(params=params)
        # DataFrameに変換
        df = pd.json_normalize(meta_info.get("GET_META_INFO",{}).get("METADATA_INF",{}).get("TABLE_INF",[]))   
        return df
    
    # DataCatalogを取得してDataFrameを返す
    def get_catalog_df(self, params: dict ={}) -> pd.DataFrame:
        """
        3.7. データカタログ情報取得
        """
        catalog = self.get_catalog(params=params)
        # DataFrameに変換
        df = pd.json_normalize(catalog.get("GET_DATA_CATALOG",{}).get("DATA_CATALOG_LIST_INF",{}).get("DATA_CATALOG_INF",[]))   
        return df


    
def init_env():
    # .envファイルから環境変数を読み込む
    dotenv_path = os.environ.get("DOTENV_PATH", None)
    if dotenv_path is None:
        load_dotenv()
    else:
        load_dotenv(dotenv_path)


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    init_env()
    app_id = os.getenv("JPESTAT_APP_ID", "")
    if not app_id:
        raise ValueError("JPESTAT_APP_ID is not set in the environment variables.")
    client = JPEStatClient(app_id=app_id, lang="J")
    params = {
        "surveyYears": "2020",
    }
    # 統計表情報を取得
    data_json =  client.get_stat_data_json(params={"statsDataId": "0004014855"})
    print(json.dumps(data_json, indent=4, ensure_ascii=False))
    data_object: JPEStatData = client.get_stat_data_object(params={"statsDataId": "0004014855"})
    df = data_object.get_values_df()
    print(df.head())
