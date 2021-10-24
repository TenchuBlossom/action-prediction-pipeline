import pandas as pd
import numpy

a = [[1, 2], [3, 4]]
df1 = pd.DataFrame(a, columns=['A', 'B'])
df2 = pd.DataFrame(None, columns=['A', 'B', 'C'])

df3 = pd.concat([df1, df2])

b = 0