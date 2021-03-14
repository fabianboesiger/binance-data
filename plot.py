import numpy as np
import matplotlib.pyplot as plt
import csv

import csv

def moving_average(x, w):
    return np.convolve(x, np.ones(w), "same") / w

mov = []
val = []

with open("data.csv", newline="") as csvfile:
    spamreader = csv.reader(csvfile, delimiter=",")

    for row in spamreader:
        if len(mov) < 10000:
            mov.append(float(row[0]))
            val.append(float(row[1]))

dmov = np.ediff1d(mov)
admov = dmov - moving_average(dmov, 1000)
dval = np.ediff1d(val)
adval = dval - moving_average(dval, 1000)

(x, _, _, _) = np.linalg.lstsq(np.reshape(np.cumsum(admov), (len(admov), 1)), np.cumsum(adval), rcond=None)

fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1)
fig.suptitle('Title')

ax1.plot(val, '-')
ax1.set_ylabel('Market Value')

vi = moving_average(dval - dmov * x, 1000)

ax3.plot(vi, '-')
ax3.set_ylabel('Value Difference')


ax2.plot(val[0] + np.cumsum(dmov * x), '-')
ax2.set_ylabel('Intrinsic Value')

sma = moving_average(np.cumsum(dval), 200)
lma = moving_average(np.cumsum(dval), 1000)
llma = moving_average(np.cumsum(dval), 2000)

ax4.plot(sma, '-')
ax4.plot(lma, '-')
ax4.plot(llma, '-')
ax4.set_ylabel('Trend')

bought = []
pos = False
boughtAt = 0
capital = 1
comp = []

for i in range(1, len(dval)):
    if vi[i] < 0 and sma[i] >= lma[i] and sma[i - 1] < lma[i - 1] and llma[i - 1] < llma[i] and pos == False and i > 1000 and i < 9000:
        pos = True
        boughtAt = val[i]
    if sma[i] <= lma[i] and sma[i - 1] > lma[i - 1] and pos == True:
        pos = False
        capital = capital / boughtAt * val[i]
    bought.append(capital)
    comp.append(val[i] / val[0])

ax5.plot(bought, '-')
ax5.plot(comp, '-')
ax5.set_ylabel('Capital')

plt.show()