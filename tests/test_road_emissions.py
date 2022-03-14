import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest
# adding the path of the package to load modules
# this makes all functions available
adp_path = str(Path(sys.path[0]).parent.absolute() / "aeriusdatapipeline")
sys.path.append(adp_path)
import road_emissions as road


# test_code_data = [
#     (['london', 'wales', 'london'], 3),
#     (['lon', 'lon', 'lon'], 3),
#     (['l', 'w', 'l'], 3),
#     (['h h', 'h h h h', 'h h h h h h '], 3),
#     ([' hi', '  hi'], 2)
# ]


# @pytest.mark.parametrize('sample, expected', test_code_data)
# def test_make_code_unique(sample, expected):
#     test_df = pd.DataFrame(sample, columns=['name'])
#     out_df = road._make_code(test_df)
#     assert len(out_df['code'].unique()) == expected


# @pytest.mark.parametrize('sample, expected', test_code_data)
# def test_make_code_cols(sample, expected):
#     test_df = pd.DataFrame(sample, columns=['name'])
#     out_df = road._make_code(test_df)
#     assert out_df.columns.to_list() == ['name', 'code']


# @pytest.mark.parametrize('sample, expected', test_code_data)
# def test_make_code_len_max(sample, expected):
#     test_df = pd.DataFrame(sample, columns=['name'])
#     out_df = road._make_code(test_df)
#     codes = out_df['code'].to_list()
#     assert max(len(x) for x in codes) <= 4


# @pytest.mark.parametrize('sample, expected', test_code_data)
# def test_make_code_len_min(sample, expected):
#     test_df = pd.DataFrame(sample, columns=['name'])
#     out_df = road._make_code(test_df)
#     codes = out_df['code'].to_list()
#     assert min(len(x) for x in codes) > 0


# df_for_expand = [
#     (pd.DataFrame({'a': [1, 2]}), [0]),
#     (pd.DataFrame({'a': [1, 2]}), [0, 1, -1]),
#     (pd.DataFrame({'a': [1, 2, 4, 5, 6, 7]}), [0, 1, 2]),
#     (pd.DataFrame({'a': [1, 2],
#                    'b': [3, 4]}), [1, 2, 3])
# ]
# # these will fail test_expand_feature_unique
# df_for_expand_ext = [
#     (pd.DataFrame({'a': [1, 2]}), [1, 1]),
#     (pd.DataFrame(), [0, 1])
# ]


# @pytest.mark.parametrize('data, multiply', df_for_expand + df_for_expand_ext)
# def test_expand_feature_len(data, multiply):
#     exp_df = road.expand_feature(data, 'x', multiply)
#     assert len(exp_df) == len(data) * len(multiply)


# @pytest.mark.parametrize('data, multiply', df_for_expand)
# def test_expand_feature_unique(data, multiply):
#     exp_df = road.expand_feature(data, 'x', multiply)
#     assert len(exp_df['x'].unique()) == len(multiply)


# @pytest.mark.parametrize('data, multiply', df_for_expand)
# def test_expand_feature_unique_rows(data, multiply):
#     exp_df = road.expand_feature(data, 'x', multiply)
#     # the first column isnt names so gets a default value of 0
#     assert exp_df.groupby(['a', 'x']).size().max() == 1


nh3_data = [
    pd.DataFrame({'HGV': [100, np.NaN, np.NaN, 100, 100],
                  'Bus and Coach': [np.NaN, 100, np.NaN, np.NaN, np.NaN],
                  'emission factor': [3, 4, 4, np.NaN, 4]})
    ]


@pytest.mark.parametrize('data', nh3_data)
def test_prep_nh3_data_na(data):
    df_out = road.prep_nh3_data(data)
    assert df_out[['maximum_speed', 'gradient']].isna().sum().sum() < 1


@pytest.mark.parametrize('data', nh3_data)
def test_prep_nh3_data_unique(data):
    df_out = road.prep_nh3_data(data)
    assert len(df_out.drop_duplicates()) == len(df_out)


@pytest.mark.parametrize('data', nh3_data)
def test_prep_nh3_data_new_col(data):
    df_out = road.prep_nh3_data(data)
    assert df_out.shape[1] == data.shape[1] + 2


@pytest.mark.parametrize('data', nh3_data)
def test_prep_nh3_data_empty(data):
    df_out = road.prep_nh3_data(data)
    assert df_out.shape[0] > 0


# nox_data = [
#     (np.array([[100, np.NaN, np.NaN, 34, 0],
#               [np.NaN, 100, np.NaN, 0, 0],
#               [np.NaN, np.NaN, 100, 2, 0],
#               [100, np.NaN, np.NaN, 100, 0],
#               [100, np.NaN, np.NaN, 100, 7],
#               [100, np.NaN, np.NaN, 100, np.NaN]]),
#         ['car', 'Bus and Coach', 'HGV', 'emission_factor', 'gradient'])
#     ]


# @pytest.mark.parametrize('data, cols', nox_data)
# def test_prep_nox_data_rows(data, cols):
#     df = pd.DataFrame(data, columns=cols)
#     df_out = road.prep_nox_data(df)
#     assert df_out.shape[0] == 2


# @pytest.mark.parametrize('data, cols', nox_data)
# def test_prep_nox_data_unique(data, cols):
#     df = pd.DataFrame(data, columns=cols)
#     df_out = road.prep_nox_data(df)
#     assert len(df_out.drop_duplicates()) == len(df_out)


# @pytest.mark.parametrize('data, cols', nox_data)
# def test_prep_nox_data_na(data, cols):
#     df = pd.DataFrame(data, columns=cols)
#     df_out = road.prep_nox_data(df)
#     assert df_out[['car', 'emission_factor', 'gradient']].isna().sum().sum() < 1


# hdv_data = [
#     (np.array([['Up Hill', 12, 1],
#               [np.NaN, 12, 0],
#               ['Down Hill', 2, 2],
#               ['Up Hill', 2, 2]]),
#         ['Flow Direction', 'emission_factor', 'gradient'])
#     ]


# @pytest.mark.parametrize('data, cols', hdv_data)
# def test_prep_hdv_data_unique(data, cols):
#     df = pd.DataFrame(data, columns=cols)
#     df.replace('nan', np.NaN, inplace=True)
#     df_out = road.prep_hdv_data(df)
#     assert len(df_out.drop_duplicates()) == len(df_out)


# @pytest.mark.parametrize('data, cols', hdv_data)
# def test_prep_hdv_data_dtype(data, cols):
#     df = pd.DataFrame(data, columns=cols)
#     df.replace('nan', np.NaN, inplace=True)
#     df_out = road.prep_hdv_data(df)
#     assert df_out.dtypes.to_list() ==\
#         [np.dtype('int64'), np.dtype('float64'), np.dtype('int64')]


# road_data = [
#     (np.array([[100, np.NaN, np.NaN, 34, '0'],
#                [np.NaN, 100, np.NaN, '0', 0],
#                [np.NaN, 100, np.NaN, 3.5, 0],
#                [np.NaN, np.NaN, 100, 10, 2.5]]),
#         np.array([[100, np.NaN, 12, 34],
#                   [np.NaN, 100, 12, 34]]),
#         np.array([[100, np.NaN, 12, 34],
#                   [np.NaN, 100, 12, 34]]))
#                ]


# #@pytest.mark.parametrize('nh3, nox, hdv', road_data)
# def test_read_road_data(nh3, nox, hdv):
#     df_nh3 = pd.DataFrame(nh3, columns=['Car', 'Motorcycle', 'LGV', 'maximum_speed', 'year'])
#     df_nox = pd.DataFrame(nox, columns=['Car', 'Taxi (black cab)', 'maximum_speed', 'year'])
#     df_hdv = pd.DataFrame(hdv, columns=['HGV', 'Bus and Coach', 'maximum_speed', 'year'])
#     print(df_1)
#     print(df_2)
#     print(df_3)
#     quit()
#     df_out = road.read_road_data(df_nh3, df_nox, df_hdv)
#     #assert len(df_out) == len(df_nh3) + len(df_nox) + len(df_hdv)
