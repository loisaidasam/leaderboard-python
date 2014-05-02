import unittest
from leaderboard_test import LeaderboardTest
from tie_ranking_leaderboard_test import TieRankingLeaderboardTest

def all_tests():
  suite = unittest.TestSuite()
  suite.addTest(unittest.makeSuite(LeaderboardTest))
  suite.addTest(unittest.makeSuite(TieRankingLeaderboardTest))
  return suite
