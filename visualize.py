import matplotlib.pyplot as plt
from statistics import mean, stdev
import inflect


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
