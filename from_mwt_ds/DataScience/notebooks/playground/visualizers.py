import matplotlib.pyplot as plt
import seaborn as sns

def new_ax():
    _,ax = plt.subplots(dpi=100, figsize=[9,4])
    return ax


def plot_reward(_, job, ax=None, window=10):
    ax = ax or new_ax()
    ax.set_title('Average reward')
    df = job.loss_table.rolling(window=window).mean()
    p = sns.lineplot(x = df.reset_index('file').index, y= -df['loss'], label='Reward', errorbar=None, sort=False, estimator=None, ax=ax)
    p.set_ylabel("reward")
    p.set(ylim=(0, 1))
    ax.legend(loc='center left', bbox_to_anchor=(0.75, 0.5))


class TrackIt:
    def __init__(self, extractor, title):
        self._values = []
        self._title = title
        self._extractor = extractor

    def __call__(self, _, job, ax=None, window=10):
        self._values.append(self._extractor(job))
        ax.set_title(self._title)
        p = sns.lineplot(x = range(len(self._values)), y= self._values, label=self._title, errorbar=None, sort=False, estimator=None, ax=ax)
        p.set_ylabel(self._title)
        ax.legend(loc='center left', bbox_to_anchor=(0.75, 0.5))