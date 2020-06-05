import uuid
from datetime import datetime, timedelta
from random import gauss, seed, randint, uniform

class DataGenerator():
	def __init__(self, num_users=6000000):
		self.uids = self.generate_uids(num_users)

	def get_users(self):
		return self.uids

	def generate_uids(self, num_users):
		users = [{
			'UID': str(uuid.uuid4()),
			'Age': self.generate_age(),
			'StartedPlaying': None
		} for i in range(num_users)]

		return users

	def get_game(self):
		return "Wii Fit"

	def get_uid(self):
		user = self.uids[randint(0, len(self.uids) - 1)]
		if user['StartedPlaying']:
			delta = datetime.now() - user['StartedPlaying']
			r = gauss(0.5, 0.4)
			if (delta.seconds * delta.days > 30 * 60 and r < 0.8) or r < 0.2:
				user['StartedPlaying'] = None
		else:
			user['StartedPlaying'] = datetime.now()
		return user['UID']

	def generate_age(self):
		return int(gauss(0.75, 0.15) * uniform(22, 70))

	def generate_data(self):
		uid = self.get_uid()
		time = datetime.now()
		game = 1#get_game()
		platform = 1#get_platform(game)
		platform_stats = 1#generate_platform_stats(platform)

		return {
			'UID': uid,
			'Time': time,
			'Game': game,
			'Platform': platform,
			'PlatformStats': platform_stats,
		}

if __name__=="__main__":
	seed()
	generator = DataGenerator(100)
	while True:
		print(generator.generate_data())