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

    fig, ax = plt.subplots(ncols=2, nrows=3, figsize=(10, 5))
    print(elapsed_times)
    ax[0][0].plot(range(1, nTest+1), list(elapsed_times.values())[0])
    ax[1][0].plot(range(1, nTest+1), list(elapsed_times.values())[1])
    ax[0][1].plot(range(1, nTest+1), list(elapsed_times.values())[2])
    ax[1][1].plot(range(1, nTest+1), list(elapsed_times.values())[3])
    ax[2][0].plot(range(1, nTest+1), list(elapsed_times.values())[4])
    ax[2][1].plot(range(1, nTest+1), list(elapsed_times.values())[5])

    max_height = max([max(times)
                     for times in list(elapsed_times.values())])
    min_height = min([max(times)
                     for times in list(elapsed_times.values())])

    space_range = max_height - min_height
    offset = space_range * 0.8

    ax[0][0].set_ylim([min_height - (offset/2), max_height + (offset/2)])
    ax[0][1].set_ylim([min_height - (offset/2), max_height + (offset/2)])
    ax[1][0].set_ylim([min_height - (offset/2), max_height + (offset/2)])
    ax[1][1].set_ylim([min_height - (offset/2), max_height + (offset/2)])

    annot_max(range(1, nTest+1), list(elapsed_times.values())[0], ax[0][0])
    annot_max(range(1, nTest+1), list(elapsed_times.values())[1], ax[1][0])
    annot_max(range(1, nTest+1), list(elapsed_times.values())[2], ax[0][1])
    annot_max(range(1, nTest+1), list(elapsed_times.values())[3], ax[1][1])
    annot_max(range(1, nTest+1), list(elapsed_times.values())[4], ax[2][0])
    annot_max(range(1, nTest+1), list(elapsed_times.values())[5], ax[2][1])

    # ax[0][0].set_yscale('log')
    # ax[0][1].set_yscale('log')
    # ax[1][0].set_yscale('log')
    # ax[1][1].set_yscale('log')

    # Calcolo della media e della stdev del tempo di esecuzione al variare della dimensione del dataset
    mean_elapsed_time = [mean(mean_el)
                         for mean_el in list(elapsed_times.values())]
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


def barplot_times_to_compare_diff_implem(data, nJob):
    # Convert the number of jobs in an ordinal number
    p = inflect.engine()

    # Calcolo delle percentuali del dataset utilizzate
    percentages = [int(key) for key in data["map-reduce"].keys()]

    # Preparazione dei dati per le barre
    map_reduce_times = list(data["map-reduce"].values())
    spark_core_times = list(data["spark-core"].values())
    spark_sql_times = list(data["spark-sql"].values())
    hive_times = list(data["hive"].values())

    spark_core_times = [
        float(value) if value != "N/A" else np.nan for value in spark_core_times]
    spark_sql_times = [
        float(value) if value != "N/A" else np.nan for value in spark_sql_times]

    map_reduce_times = [
        float(value) if value != "N/A" else np.nan for value in map_reduce_times]
    hive_times = [
        float(value) if value != "N/A" else np.nan for value in hive_times]

    # Creazione del bar plot
    bar_width = 0.2
    index = range(len(percentages))

    plt.bar(index, map_reduce_times, bar_width, label="Map-Reduce")
    plt.bar([i + bar_width for i in index],
            hive_times, bar_width, label="Hive")
    plt.bar([i + 2 * bar_width for i in index],
            spark_core_times, bar_width, label="Spark-Core")
    plt.bar([i + 3 * bar_width for i in index],
            spark_sql_times, bar_width, label="Spark-SQL")

    # Configurazione dell'asse x
    plt.xlabel("Dataset percentage %")
    plt.ylabel("Execution time (seconds)")
    plt.xticks([i + 1.5 * bar_width for i in index], percentages)

    # plt.yticks(range(0, int(max(hive_times))+20, 20))

    plt.title("Execution time of the " + p.ordinal(nJob) + " job")

    # Aggiunta della legenda
    plt.legend()

    # Mostrare il grafico
    plt.tight_layout()
    plt.show()


def barplot_times_to_compare_local_AWS(data, implementation):
    # Convert the number of jobs in an ordinal number
    p = inflect.engine()

    # Calcolo delle percentuali del dataset utilizzate
    percentages = [int(key) for key in data["first-local"].keys()]

    # Preparazione dei dati per le barre
    first_job_local_times = list(data["first-local"].values())
    first_job_AWS_times = list(data["first-AWS"].values())
    second_job_local_times = list(data["first-local"].values())
    second_job_AWS_times = list(data["first-AWS"].values())

    # Creazione del bar plot
    bar_width = 0.2
    index = range(len(percentages))

    plt.bar(index, first_job_local_times, bar_width,
            label=f"{p.ordinal(1)}-job-local")
    plt.bar([i + bar_width for i in index],
            first_job_AWS_times, bar_width, label=f"{p.ordinal(1)}-job-AWS")
    plt.bar([i + 2 * bar_width for i in index],
            second_job_local_times, bar_width, label=f"{p.ordinal(2)}-job-local")
    plt.bar([i + 3 * bar_width for i in index],
            second_job_AWS_times, bar_width, label=f"{p.ordinal(2)}-job-AWS")

    # Configurazione dell'asse x
    plt.xlabel("Dataset percentage %")
    plt.ylabel("Execution time (minutes)")
    plt.xticks([i + 1.5 * bar_width for i in index], percentages)

    # plt.yticks(range(0, int(max(hive_times))+20, 20))

    plt.title(f"Comparization of the execution times in {implementation}")

    # Aggiunta della legenda
    plt.legend()

    # Mostrare il grafico
    plt.show()
