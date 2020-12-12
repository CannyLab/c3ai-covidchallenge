import matplotlib.pyplot as plt
import seaborn as sns

def barplot(series, figsize=(10,5), orient='h'):
  #series = series.value_counts()
  if orient == 'h':
    plt.figure(figsize=(figsize[1], figsize[0]))
    ax = sns.barplot(y=series.index.values, x=series, orient='h')
    ax.set_xticklabels(ax.get_xticklabels(), 
                      rotation=90, 
                      horizontalalignment='right')
  elif orient == 'v':

    plt.figure(figsize=figsize)

    ax = sns.barplot(x=series.index.values, y=series, orient='v')
    ax.set_xticklabels(ax.get_xticklabels(), 
                      rotation=90, 
                      horizontalalignment='right');
