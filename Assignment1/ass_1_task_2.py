# Task 2 - Vinicius Pinho and Gabriel Vaz Cançado Ferreira

import pandas as pd

from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio

df = pd.read_csv('./moviedata.csv')

print(df)
# ==========================================================================================

# a - Inspecting the DF

print("DataFrame shape: ", df.shape)
print("\n")

print("DataFrame columns: ", df.columns)
print("\n")

print("DataFrame info: ", df.info)
print("\n")

print("DataFrame describe: ", df.describe())
print("\n")

print("First five records: ", df.head(5))
print("\n")

print("Last five records: ", df.tail(5))
print("\n")

# ==========================================================================================

# b - Selecting Columns

print("First five records trimmed: ", df.loc[:, ['movie_title', 'duration', 'num_voted_users']].head(5))
print("\n")

# ==========================================================================================

# c - Selecting first five action movies

action_movies = df[df["genres"] == "Action"]

print("First five action records: ", action_movies.loc[:, ['movie_title', 'genres']].head(5))
print("\n")

# ==========================================================================================

# d - Top 10 Scored Movies

print("Highest rated action movies on IMDB: ", action_movies.sort_values(by="imdb_score", ascending=False).loc[:, ['movie_title', 'imdb_score']].head(10))
print("\n")
# ==========================================================================================

# e - Top 10 highest gross mean directors

gross_score_by_directors = df.groupby("director_name")["gross"].mean().sort_values(ascending=False).head(10)
print("Directors with highest mean gross: ", gross_score_by_directors)
print("\n")
# ==========================================================================================

# f - Deleting empty rows

df_clean = df.dropna()
print("Dataframe without rows that have at least one empty value:", df_clean)
print("\n")
# ==========================================================================================

# Visualize parts of the data using pandas:plotting:scatter matrix and DataFrameGroupBy:hist

selected_cols = [
    'imdb_score',
    'duration',
    'budget',
    'gross',
    'num_critic_for_reviews',
    'num_user_for_reviews',
    'num_voted_users'
]

df_selected = df[selected_cols].dropna()

fig_matrix = px.scatter_matrix(df_selected,
                                dimensions=selected_cols,
                                title="Scatter Matrix - Atributos de Filmes",
                                height=900)
fig_matrix.update_traces(diagonal_visible=True)
fig_matrix.show()

for col in selected_cols:
    fig_hist = px.histogram(df_selected, x=col,
                            nbins=40,
                            title=f"Histograma - {col}",
                            marginal='box') 
    fig_hist.show()

# ==========================================================================================