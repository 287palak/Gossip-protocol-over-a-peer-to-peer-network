import unittest
from seed import SeedNode

class TestSeedNode(unittest.TestCase):
    def setUp(self):
        # Initialize SeedNode object with n=2
        self.seed = SeedNode("127.0.0.1", 8888, 2)

    def test_register_peer(self):
        # Test registration of a new peer
        self.seed.handle_registration("192.168.1.1", 12345)
        self.assertIn(("192.168.1.1", 12345), self.seed.peer_list)

    def test_remove_dead_node(self):
        # Test removal of a dead node from the peer list
        self.seed.peer_list[("192.168.1.2", 54321)] = "192.168.1.2:54321"
        self.seed.handle_dead_node("192.168.1.2", 54321)
        self.assertNotIn(("192.168.1.2", 54321), self.seed.peer_list)

if __name__ == '__main__':
    unittest.main()
