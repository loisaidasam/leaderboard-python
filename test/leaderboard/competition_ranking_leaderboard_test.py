from redis import Redis, StrictRedis, ConnectionPool
from leaderboard.competition_ranking_leaderboard import CompetitionRankingLeaderboard
import unittest
import time
import sure


class CompetitionRankingLeaderboardTest(unittest.TestCase):

    def setUp(self):
        self.leaderboard = CompetitionRankingLeaderboard('ties')

    def tearDown(self):
        self.leaderboard.redis_connection.flushdb()

