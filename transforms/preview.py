import dask.dataframe as dd

class Transform:

    def __init__(self, config):
        self.config = config

    def __call__(self, datasets: dict) -> dict:

       for key, data in datasets.items():
           total = len(data['data']['CB2_CAMP'])
           totals =  data['data'].map_partitions(lambda df: df.shape[0]).compute()
           totals = data['data'].partitions[0].shape[0].compute()
           total = data['data'].shape[0].compute()
           a = 0
