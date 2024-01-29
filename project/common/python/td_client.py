import pytd
import pandas as pd

class TdClient:
    def __init__(self):
        self.client = pytd.Client()

    def load_td_from_dataframe(self, output_df:pd.core.frame.DataFrame, database:str, table:str, mode:str) -> None:
        """
        指定されたデータフレームをTreasureDataに格納する
        
        Args:
            output_df(pd.core.frame.DataFrame): 格納するデータフレーム
            database(str): 格納先データベース
            table(str): 格納先テーブル
            mode(str): データの保存方法('error','overwrite','append','ignore')
        Returns:
            None
        """
        self.client.load_table_from_dataframe(
            dataframe=output_df,
            destination=f'{database}.{table}',
            writer='bulk_import',
            if_exists=mode,
            fmt='msgpack'
            )
