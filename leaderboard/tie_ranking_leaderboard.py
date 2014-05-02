from .leaderboard import Leaderboard
from redis import StrictRedis, Redis, ConnectionPool
import math
from itertools import izip_longest

class TieRankingLeaderboard(Leaderboard):
    DEFAULT_TIES_NAMESPACE = 'ties'

    def __init__(self, leaderboard_name, **options):
        '''
        Initialize a connection to a specific leaderboard. By default, will use a
        redis connection pool for any unique host:port:db pairing.

        The options and their default values (if any) are:

        host : the host to connect to if creating a new handle ('localhost')
        port : the port to connect to if creating a new handle (6379)
        db : the redis database to connect to if creating a new handle (0)
        page_size : the default number of items to return in each page (25)
        connection : an existing redis handle if re-using for this leaderboard
        connection_pool : redis connection pool to use if creating a new handle
        '''
        super(TieRankingLeaderboard, self).__init__(leaderboard_name, **options)

        self.leaderboard_name = leaderboard_name
        self.options = options

        self.ties_namespace = self.options.pop(
            'ties_namespace',
            self.DEFAULT_TIES_NAMESPACE)

    def rank_member_in(
            self, leaderboard_name, member, score, member_data=None):
        '''
        Rank a member in the named leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @param member [String] Member name.
        @param score [float] Member score.
        @param member_data [String] Optional member data.
        '''
        pipeline = self.redis_connection.pipeline()
        if isinstance(self.redis_connection, Redis):
            pipeline.zadd(leaderboard_name, member, score)
            pipeline.zadd(self._ties_leaderboard_key(leaderboard_name), str(float(score)), score)
        else:
            pipeline.zadd(leaderboard_name, score, member)
            pipeline.zadd(self._ties_leaderboard_key(leaderboard_name), score, str(float(score)))
        if member_data:
            pipeline.hset(
                self._member_data_key(leaderboard_name),
                member,
                member_data)
        pipeline.execute()

    def ranked_in_list_in(self, leaderboard_name, members, **options):
        '''
        Retrieve a page of leaders from the named leaderboard for a given list of members.

        @param leaderboard_name [String] Name of the leaderboard.
        @param members [Array] Member names.
        @param options [Hash] Options to be used when retrieving the page from the named leaderboard.
        @return a page of leaders from the named leaderboard for a given list of members.
        '''
        ranks_for_members = []

        pipeline = self.redis_connection.pipeline()

        for member in members:
            if self.order == self.ASC:
                pipeline.zrank(leaderboard_name, member)
            else:
                pipeline.zrevrank(leaderboard_name, member)

            pipeline.zscore(leaderboard_name, member)

        responses = pipeline.execute()

        for index, member in enumerate(members):
            data = {}
            data[self.MEMBER_KEY] = member

            score = responses[index * 2 + 1]
            if score is not None:
                score = float(score)
            data[self.SCORE_KEY] = score

            if self.order == self.ASC:
                data[self.RANK_KEY] = self.redis_connection.zrank(self._ties_leaderboard_key(leaderboard_name), str(data[self.SCORE_KEY]))
            else:
                data[self.RANK_KEY] = self.redis_connection.zrevrank(self._ties_leaderboard_key(leaderboard_name), str(data[self.SCORE_KEY]))
            if data[self.RANK_KEY] is not None:
                data[self.RANK_KEY] += 1

            if ('with_member_data' in options) and (True == options['with_member_data']):
                data[
                    self.MEMBER_DATA_KEY] = self.member_data_for_in(
                    leaderboard_name,
                    member)

            ranks_for_members.append(data)

        if 'sort_by' in options:
            if self.RANK_KEY == options['sort_by']:
                ranks_for_members = sorted(
                    ranks_for_members,
                    key=lambda member: member[
                        self.RANK_KEY])
            elif self.SCORE_KEY == options['sort_by']:
                ranks_for_members = sorted(
                    ranks_for_members,
                    key=lambda member: member[
                        self.SCORE_KEY])

        return ranks_for_members

    def _ties_leaderboard_key(self, leaderboard_name):
        '''
        Key for ties leaderboard.

        @param leaderboard_name [String] Name of the leaderboard.
        @return a key in the form of +leaderboard_name:ties_namespace+
        '''
        return '%s:%s' % (leaderboard_name, self.ties_namespace)