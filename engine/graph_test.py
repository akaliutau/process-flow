import unittest

from engine.exceptions import ValidationException
from graph import DNode, Graph


class TestingGraph(unittest.TestCase):

    def test_base_functions(self):
        node_1 = DNode('1')
        node_2 = DNode('2')
        node_3 = DNode('3')
        node_4 = DNode('4')
        node_5 = DNode('5')

        node_1 >> node_2
        node_2 >> node_3
        node_2 >> node_4
        node_4 >> node_3
        node_1 >> node_5
        node_5 >> node_4

        g = Graph([node_1, node_2, node_3, node_4, node_5])
        g.prepare()
        while g.has_next():
            print(g.next().task_id)

    def test_graph_with_cycle(self):
        node_1 = DNode('1')
        node_2 = DNode('2')
        node_3 = DNode('3')
        node_4 = DNode('4')
        node_5 = DNode('1')

        node_1 >> node_2
        node_1 >> node_3
        node_2 >> node_3
        node_3 >> node_1
        node_3 >> node_4

        g = Graph([node_1, node_2, node_3, node_4, node_5])
        self.assertRaises(ValidationException, lambda: g.prepare())

    def test_self_cycle(self):
        node_1 = DNode('1')

        node_1 >> node_1

        g = Graph([node_1])
        self.assertRaises(ValidationException, lambda: g.prepare())

    def test_multi_part_graph(self):
        node_1 = DNode('1')
        node_2 = DNode('2')
        node_3 = DNode('3')
        node_4 = DNode('4')

        node_1 >> node_2
        node_3 >> node_4

        g = Graph([node_1, node_2, node_3, node_4])
        g.prepare()


if __name__ == '__main__':
    unittest.main()
