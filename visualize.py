import matplotlib.pyplot as plt
from statistics import mean, stdev
import numpy as np
import inflect


def annot_max(x, y, ax=None):
    y = np.array(y)
    xmax = x[np.argmax(y)]
    ymax = y.max()
    text = "max time: {:.3f}s".format(ymax)
    if not ax:
        ax = plt.gca()
    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    arrowprops = dict(
        arrowstyle="->", connectionstyle="angle,angleA=0,angleB=60")
    kw = dict(xycoords='data', textcoords="axes fraction",
              arrowprops=arrowprops, bbox=bbox_props, ha="right", va="top")
    ax.annotate(text, xy=(xmax, ymax), xytext=(0.94, 0.96), **kw)


def plot_statistics(nTest, elapsed_times, nJob, implementation, dataset_percentage):
    # Plotting the statistics
    # elapsed times is a list of lists of 4 elements

    # Convert the number of jobs in an ordinal number
    p = inflect.engine()

    try:
        assert len(elapsed_times) == 4
    except AssertionError:
        print("Error: elapsed_times must be a list of 4 elements")
        return

    fig, ax = plt.subplots(ncols=2, nrows=2, figsize=(10, 5))
    ax[0][0].plot(range(1, nTest+1), elapsed_times[0])
    ax[1][0].plot(range(1, nTest+1), elapsed_times[1])
    ax[0][1].plot(range(1, nTest+1), elapsed_times[2])
    ax[1][1].plot(range(1, nTest+1), elapsed_times[3])

    max_height = max([max(elapsed_times[0]), max(elapsed_times[1]), max(
        elapsed_times[2]), max(elapsed_times[3])])
    min_height = min([min(elapsed_times[0]), min(elapsed_times[1]), min(
        elapsed_times[2]), min(elapsed_times[3])])

    space_range = max_height - min_height
    offset = space_range * 0.8

    ax[0][0].set_ylim([min_height - (offset/2), max_height + (offset/2)])
    ax[0][1].set_ylim([min_height - (offset/2), max_height + (offset/2)])
    ax[1][0].set_ylim([min_height - (offset/2), max_height + (offset/2)])
    ax[1][1].set_ylim([min_height - (offset/2), max_height + (offset/2)])

    annot_max(range(1, nTest+1), elapsed_times[0], ax[0][0])
    annot_max(range(1, nTest+1), elapsed_times[1], ax[1][0])
    annot_max(range(1, nTest+1), elapsed_times[2], ax[0][1])
    annot_max(range(1, nTest+1), elapsed_times[3], ax[1][1])

    # ax[0][0].set_yscale('log')
    # ax[0][1].set_yscale('log')
    # ax[1][0].set_yscale('log')
    # ax[1][1].set_yscale('log')

    # Calcolo della media e della stdev del tempo di esecuzione al variare della dimensione del dataset
    mean_elapsed_time = [mean(mean_el) for mean_el in elapsed_times]
    # stdev_elapsed_time = [stdev(stdev_el) for stdev_el in elapsed_times]

    ax[0][0].plot(range(1, nTest+1),
                  [mean_elapsed_time[0] for _ in range(1, nTest+1)])
    ax[1][0].plot(range(1, nTest+1),
                  [mean_elapsed_time[1] for _ in range(1, nTest+1)])
    ax[0][1].plot(range(1, nTest+1),
                  [mean_elapsed_time[2] for _ in range(1, nTest+1)])
    ax[1][1].plot(range(1, nTest+1),
                  [mean_elapsed_time[3] for _ in range(1, nTest+1)])

    ax[0][0].set_xlabel('Test number')
    ax[1][0].set_xlabel('Test number')
    ax[0][1].set_xlabel('Test number')
    ax[1][1].set_xlabel('Test number')
    ax[0][0].set_ylabel('Time [s]')
    ax[1][0].set_ylabel('Time [s]')
    ax[0][1].set_ylabel('Time [s]')
    ax[1][1].set_ylabel('Time [s]')
    ax[0][0].set_title("Execution time of the " + p.ordinal(nJob) +
                       " job" + " with 100% of the dataset")
    ax[0][1].set_title("Execution time of the " + p.ordinal(nJob) +
                       " job" + " with 150% of the dataset")
    ax[1][0].set_title("Execution time of the " + p.ordinal(nJob) +
                       " job" + " with 200% of the dataset")
    ax[1][1].set_title("Execution time of the " + p.ordinal(nJob) +
                       " job" + " with 250% of the dataset")
    fig.tight_layout()
    fig.savefig("plots/" + implementation + "_" + str(nJob) +
                "_" + str(dataset_percentage) + ".png")
    # map-reduce_1_100.png dove il job è il primo e il dataset è al 100% e l'implementazione è map-reduce
    plt.show()


def barplot_execution_times(data, nJob):
    # data = {
    #     "map-reduce": {
    #         100: 36.89401485761007,
    #         200: 46.68291923205058,
    #         250: 55.94427890141806,
    #         500: 92.59772998174033,
    #         750: 101.63732999801636,
    #         1000: 188.92043952941893
    #     },
    #     "spark-core": {
    #         100: 55.90394175847372,
    #         200: 57.22622532844544,
    #         250: 58.05035197575887,
    #         500: 80.57014412562053,
    #         750: 99.09985042254131,
    #         1000: 122.45084574381511
    #     },
    #     "spark-sql": {
    #         100: 36.39262241999309,
    #         200: 43.82355433146159,
    #         250: 49.13556230545044,
    #         500: 63.73630240758259,
    #         750: 89.41670737902322,
    #         1000: 105.17730604489645
    #     },
    #     "hive": {
    #         100: 115.15361097017922,
    #         200: 100.12641709327697,
    #         250: 106.37027971903483,
    #         500: 167.28833905220034,
    #         750: 209.77795018513999,
    #         1000: 278.80231527646384
    #     }
    # }

    # Convert the number of jobs in an ordinal number
    p = inflect.engine()

    # Calcolo delle percentuali del dataset utilizzate
    percentages = [int(key) for key in data["map-reduce"].keys()]

    # Preparazione dei dati per le barre
    map_reduce_times = list(data["map-reduce"].values())
    spark_core_times = list(data["spark-core"].values())
    spark_sql_times = list(data["spark-sql"].values())
    hive_times = list(data["hive"].values())

    # Creazione del bar plot
    bar_width = 0.2
    index = range(len(percentages))

    plt.bar(index, map_reduce_times, bar_width, label="Map-Reduce")
    plt.bar([i + bar_width for i in index],
            spark_core_times, bar_width, label="Spark-Core")
    plt.bar([i + 2 * bar_width for i in index],
            spark_sql_times, bar_width, label="Spark-SQL")
    plt.bar([i + 3 * bar_width for i in index],
            hive_times, bar_width, label="Hive")

    # Configurazione dell'asse x
    plt.xlabel("Dataset percentage %")
    plt.ylabel("Execution time (seconds)")
    plt.xticks([i + 1.5 * bar_width for i in index], percentages)

    # plt.yticks(range(0, int(max(hive_times))+20, 20))

    plt.title("Execution time of the " + p.ordinal(nJob) + " job")

    # Aggiunta della legenda
    plt.legend()

    # Mostrare il grafico
    plt.show()
