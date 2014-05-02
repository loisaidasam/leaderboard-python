from redis import Redis, StrictRedis, ConnectionPool
from leaderboard.tie_ranking_leaderboard import TieRankingLeaderboard
import unittest
import time
import sure

class TieRankingLeaderboardTest(unittest.TestCase):
    def setUp(self):
        self.leaderboard = TieRankingLeaderboard('ties')

    def tearDown(self):
        self.leaderboard.redis_connection.flushdb()

    def test_version(self):
        TieRankingLeaderboard.VERSION.should.equal('2.8.0')

    def test_leaders(self):
        self.leaderboard.rank_member('member_1', 50)
        self.leaderboard.rank_member('member_2', 50)
        self.leaderboard.rank_member('member_3', 30)
        self.leaderboard.rank_member('member_4', 30)
        self.leaderboard.rank_member('member_5', 10)

        leaders = self.leaderboard.leaders(1)
        leaders[0]['rank'].should.equal(1)
        leaders[1]['rank'].should.equal(1)
        leaders[2]['rank'].should.equal(2)
        leaders[3]['rank'].should.equal(2)
        leaders[4]['rank'].should.equal(3)
