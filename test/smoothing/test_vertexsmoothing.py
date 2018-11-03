import unittest

from graphy.smoothing.vertexsmoothing import vertex_smoothing
from graphy.smoothing.utils import source_getter_tuple, target_getter_tuple, link_getter_tuple, obj_creator_tuple


class VertexSmoothingTest(unittest.TestCase):
    def test_single_input_single_output_double_hop(self):
        """
        T_ORIGINAL_TABLE -> V_TABLE -> T_TABLE_WORK -> T_TABLE_FINAL
        should become
        T_ORIGINAL_TABLE -> T_TABLE_FINAL
        """

        vertices_list = [
            ('T_ORIGINAL_DATA',),
            ('V_DATA',),
            ('T_DATA_WORK',),
            ('T_DATA_FINAL',)
        ]

        links_list = [
            ('T_ORIGINAL_DATA', 'pt', 'V_DATA'),
            ('V_DATA', 'si', 'T_DATA_WORK'),
            ('T_DATA_WORK', 'pt', 'T_DATA_FINAL')
        ]

        vertices_to_remove = filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1,
                                    map(lambda x: x[0], vertices_list))

        links_cleaned_list = vertex_smoothing(
            links_list, vertices_to_remove, source_getter_tuple, target_getter_tuple,
            link_getter_tuple, obj_creator_tuple, links_merger='-'.join
        )

        expected = [('T_ORIGINAL_DATA', 'pt-si-pt', 'T_DATA_FINAL')]

        self.assertEqual(links_cleaned_list, expected, "SISO Double Hop test failed!\n{}\n{}".format(
            links_cleaned_list, expected))

    def test_multiple_input_multiple_output_double_hop(self):
        """
        T_ORIGINAL_TABLE_1 -\                            /-> T_TABLE_FINAL_1
                             -> V_TABLE -> T_TABLE_WORK -
        T_ORIGINAL_TABLE_2 -/                            \-> T_TABLE_FINAL_2

        should become

        T_ORIGINAL_TABLE_1 -\ /-> T_TABLE_FINAL_1
                             X
        T_ORIGINAL_TABLE_2 -/ \-> T_TABLE_FINAL_2
        """

        vertices_list = [
            ("T_ORIGINAL_TABLE_1",),
            ("T_ORIGINAL_TABLE_2",),
            ("V_TABLE",),
            ("T_TABLE_WORK",),
            ("T_TABLE_FINAL_1",),
            ("T_TABLE_FINAL_2",)
        ]

        links_list = [
            ('T_ORIGINAL_TABLE_1', 'mi', 'V_TABLE'),
            ('T_ORIGINAL_TABLE_2', 'mi', 'V_TABLE'),
            ('V_TABLE', 'pt', 'T_TABLE_WORK'),
            ('T_TABLE_WORK', 'si', 'T_TABLE_FINAL_1'),
            ('T_TABLE_WORK', 'si', 'T_TABLE_FINAL_2')
        ]

        vertices_to_remove = filter(lambda x: x.startswith('V') or x.rfind('WORK') != -1,
                                    map(lambda x: x[0], vertices_list))

        links_cleaned_list = vertex_smoothing(
            links_list, vertices_to_remove, source_getter_tuple, target_getter_tuple,
            link_getter_tuple, obj_creator_tuple, links_merger='-'.join
        )

        expected = [('T_ORIGINAL_TABLE_1', 'mi-pt-si', 'T_TABLE_FINAL_1'),
                    ('T_ORIGINAL_TABLE_1', 'mi-pt-si', 'T_TABLE_FINAL_2'),
                    ('T_ORIGINAL_TABLE_2', 'mi-pt-si', 'T_TABLE_FINAL_1'),
                    ('T_ORIGINAL_TABLE_2', 'mi-pt-si', 'T_TABLE_FINAL_2')]

        self.assertEqual(links_cleaned_list, expected, "MIMO Double Hop test failed!\n{}\n{}".format(
            links_cleaned_list, expected))


if __name__ == '__main__':
    unittest.main()
