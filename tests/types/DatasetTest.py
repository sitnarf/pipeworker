import unittest
from pipeworker.types.Dataset import Dataset
from pandas import DataFrame


class DatasetTest(unittest.TestCase):

    def test_merge(self):
        dataset = Dataset(
            data=DataFrame(
                data={'col1': [1, 2], 'col2': [3, 4]}
            )
        )
        dataset2 = Dataset(
            data=DataFrame(
                data={'col3': [10, 11], 'col4': [12, 13]}
            )
        )
        self.assertTrue(dataset.merge(dataset2).data.equals(
            DataFrame(
                data={
                    'col1': [1, 2], 'col2': [3, 4],
                    'col3': [10, 11], 'col4': [12, 13]
                }
            )
        ))

    def test_merge_with_index(self):
        dataset = Dataset(
            data=DataFrame(
                data={'col1': [9, 10, 11]},
                index=[9, 10, 11]
            )
        )
        dataset2 = Dataset(
            data=DataFrame(
                data={'col2': [12, 13, 14]},
                index=[10, 11, 9]
            ),
        )
        self.assertTrue(dataset.merge(dataset2).data.equals(
            DataFrame(
                data={
                    'col1': [9, 10, 11], 'col2': [14, 12, 13],
                },
                index=[9, 10, 11]
            )
        ))

    def test_merge_with_sparse_index(self):
        dataset = Dataset(
            data=DataFrame(
                data={'col1': [10, 11, 12]},
                index=[10, 11, 12]
            )
        )
        dataset2 = Dataset(
            data=DataFrame(
                data={'col2': [12, 13]},
                index=[10, 11]
            ),
        )
        self.assertTrue(dataset.merge(dataset2).data.equals(
            DataFrame(
                data={
                    'col1': [10, 11, 12], 'col2': [12, 13, None],
                },
                index=[10, 11, 12]
            )
        ))

    def test_merge_with_data_frame(self):
        dataset = Dataset(
            data=DataFrame(
                data={'col1': [10, 11, 12]},
                index=[10, 11, 12]
            )
        )
        frame = DataFrame(
            data={'col2': [12, 13]},
            index=[10, 11]
        )
        self.assertTrue(dataset.merge(frame).data.equals(
            DataFrame(
                data={
                    'col1': [10, 11, 12], 'col2': [12, 13, None],
                },
                index=[10, 11, 12]
            )
        ))


if __name__ == '__main__':
    unittest.main()
