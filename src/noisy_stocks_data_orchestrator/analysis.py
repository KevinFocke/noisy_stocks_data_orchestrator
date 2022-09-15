"""Functions for correlating data"""
import numpy as np
from numba import jit
from numba.typed import List as NumbaList
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner


def np_mean_per_col(np_array):
    return NumbaList(np_array.mean(axis=0).tolist())  # axis 0 is over rows


def np_stdev_per_row(np_array):
    return NumbaList(np_array.std(axis=0).tolist())
    # axis 0 is over rows
    # numba requires specific type list https://numba.readthedocs.io/en/stable/reference/deprecation.html#deprecation-of-reflection-for-list-and-set-types


# BUG: Cannot assign @task to it, or will receive error; Prefect wants to pickle everything.
# TypeError: cannot pickle '_nrt_python._MemInfo' object
@jit(nopython=True)
def pearson_corr(
    dataset_stdevs,
    stocks_stdevs,
    stocks_np_array=np.array([[]]),
    dataset_np_array=np.array([[]]),
):
    # TODO: Can the types be cleaned up? Interaction between Jit & Prefect is messy.

    if stocks_np_array.shape[0] != dataset_np_array.shape[0]:
        raise ValueError("rows must be equal size!!")

    (
        stocks_array_row_count,
        stocks_array_col_count,
    ) = stocks_np_array.shape  # how many cols?
    dataset_array_row_count, dataset_array_col_count = dataset_np_array.shape

    # sanity check, are there an equal amount of rows?
    if stocks_array_row_count != dataset_array_row_count:
        raise ValueError("rows should be of equal size")

    # iterate over stocks
    correlations = NumbaList()
    for stock_col_index in range(stocks_array_col_count):
        # many iterations here thus use njit
        corr_to_stock = np.empty(dataset_array_col_count)
        # one wide series
        # cur_stock_mean = stocks_means[stock_col_index]
        cur_stock_array = stocks_np_array[
            0:, stock_col_index
        ]  # all rows, first col; in other words: current stock
        cur_stock_stdev = stocks_stdevs[stock_col_index]
        for datapoint_col_index in range(dataset_array_col_count):
            cur_datapoint_array = dataset_np_array[
                0:, datapoint_col_index
            ]  # array is empty?
            # cur_datapoint_mean = dataset_means[datapoint_col_index]
            cur_datapoint_stdev = dataset_stdevs[datapoint_col_index]
            numerator = np.cov(cur_datapoint_array, cur_stock_array, bias=True)[0][
                1
            ]  # bias True measn normalize by N
            # result is a long array
            denominator = cur_stock_stdev * cur_datapoint_stdev
            corr_to_stock[datapoint_col_index] = numerator / denominator

        # print(corr_to_stock)
        # print(corr_to_stock.shape)
        correlations.append(corr_to_stock)
    return correlations  # list containing correlations per stock
    # calc correlation


@flow(task_runner=SequentialTaskRunner())
def correlate_datasets(*args, **kwargs):
    return list(
        pearson_corr(*args, **kwargs)
    )  # convert back from NumbaList to regular Python list
