from tdigest import TDigest
from scipy.stats import ks_2samp
import matplotlib.pyplot as plt
import time

plt.figure(figsize=(8, 6), dpi=300)


def get_original_ks(ref_data, t_data):
    ks_score, _ = ks_2samp(ref_data, t_data, method='exact')
    return ks_score


def get_bins_ks(ref_data, t_data):
    max_val = max(ref_data)
    min_val = min(ref_data)

    num_bins = 1000

    step = (max_val - min_val) / num_bins

    def get_index(val):
        if val < min_val:
            bin_index = 0
        elif val > max_val:
            bin_index = num_bins + 1
        else:
            # Subtract min_val to start from 0 and then divide by step to find the appropriate bin.
            # Add 1 to skip the bin reserved for <min_val
            bin_index = 1 + int((val - min_val) / step)

            # Handle edge case where value equals max_val. It should be placed in the max-step --> max bin.
            if bin_index == num_bins + 1:
                bin_index -= 1

        return bin_index

    ref_hist = [0] * (num_bins + 2)
    new_hist = [0] * (num_bins + 2)

    for i in ref_data:
        ref_hist[get_index(i)] += 1

    for i in t_data:
        new_hist[get_index(i)] += 1

    max_count_diff = 0
    new_count = 0
    ref_count = 0

    # to align ref and test sizes.
    ref_to_test_ratio = (len(ref_data) / len(t_data))

    for i in range(len(new_hist)):
        new_count += new_hist[i] * ref_to_test_ratio
        ref_count += ref_hist[i]
        if abs(ref_count - new_count) > max_count_diff:
            max_count_diff = abs(ref_count - new_count)

    return max_count_diff / len(ref_data)


def batch_generator(data):
    """Generator function to yield batches of data for each merge in the T-DIGEST-MERGE d=20k"""
    for i in range(0, len(data), 20000):
        yield data[i:i + 20000]


def get_digest_ks(ref_data, t_data):
    """
    We calculate the KS value in case the T-DIGEST-MERGE method is used
    :param ref_data: the reference data to be compared to
    :param t_data:  the new sampled data that we want to digest
    :return: KS statistic according to T-DIGEST-MERGE
    """
    reference_digest = TDigest()
    test_digest = TDigest()

    start_time = time.time()
    reference_digest.batch_update(ref_data[:1])
    print(f"reference_digest.batch_update(ref_data) took:{time.time() - start_time} seconds to run.")

    start_time = time.time()
    for batch in batch_generator(t_data):
        temp_digest = TDigest()
        temp_digest.batch_update(batch)
        test_digest.update_from_dict(temp_digest.to_dict())
    print(f"test_digest.batch_update(ref_data) took:{time.time() - start_time} seconds to run.")

    start_time = time.time()

    max_cdf = 0
    for item in ref_data:
        curr = abs(reference_digest.cdf(item) - test_digest.cdf(item))
        if curr > max_cdf:
            max_cdf = curr
    print(f"checking the loop took:{time.time() - start_time} seconds to run.")

    return max_cdf


def get_digest_ks_run_times(ref_data, t_data):
    reference_digest = TDigest()
    test_digest = TDigest()

    start_time = time.time()
    reference_digest.batch_update(ref_data)
    print(f"reference_digest.batch_update(ref_data) took:{time.time() - start_time} seconds to run.")
    start_time = time.time()
    test_digest.batch_update(t_data)
    client_time = time.time() - start_time
    print(f"test_digest.batch_update(ref_data) took:{client_time} seconds to run.")
    start_time = time.time()

    max_cdf = 0
    for item in ref_data:
        curr = abs(reference_digest.cdf(item) - test_digest.cdf(item))
        if curr > max_cdf:
            max_cdf = curr
    loop_time = time.time() - start_time
    print(f"checking the loop took:{loop_time} seconds to run.")

    return client_time, loop_time


def get_err_array(list1, list2):
    if len(list1) != len(list2):
        raise Exception("bad arg")

    return [abs(a - b) for a, b in zip(list1, list2)]


def plot_3_methods(ground_t_l, bins_l, digest_l):
    plt.figure(figsize=(10, 6))
    k = len(ground_t_l)
    # Plot ground truth
    plt.scatter(range(k), ground_t_l, label='Exact KS statistic', marker='o')
    #
    # # Plot calculated values
    # plt.scatter(range(k), bins_l, label='Bins', marker='x')
    # plt.scatter(range(k), digest_l, label='T-Digest', marker='s')

    # Add labels, title, and legend
    plt.xlabel('Batch Index')
    plt.ylabel('Calculated Value')
    plt.title('Comparison of T-Digest and Bins Methods with exact KS value')
    plt.legend()

    # Show the plot
    plt.show()


def plot_errors(bins_error, digest_error):
    plt.figure(figsize=(10, 6))
    k = len(bins_error)
    # Plot ground truth
    plt.scatter(range(k), bins_error, label='Bins', marker='o')

    # Plot calculated values
    plt.scatter(range(k), digest_error, label='T-Digest', marker='x')

    # Add labels, title, and legend
    plt.xlabel('Batch Index')
    plt.ylabel('Error compared to exact KS statistic Value')
    plt.title('Comparison of T-Digest and Bins Methods with exact KS value')
    plt.legend()

    # Show the plot
    plt.show()


def plot_errors_lines(ground_t_l, bins_l, digest_l, figure_name="figure"):
    # Calculate the differences
    differences_bins = [est1 - true for est1, true in zip(bins_l, ground_t_l)]
    differences_digest = [est2 - true for est2, true in zip(digest_l, ground_t_l)]

    # Create a scatter plot
    # plt.figure(figsize=(10, 5))

    batches = range(1, len(ground_t_l) + 1)
    # Plot method 1
    plt.scatter(batches, differences_bins, color='blue', label='Bins', marker='o')
    # Plot method 2
    plt.scatter(batches, differences_digest, color='red', label='T-Digest', marker='x')
    #
    # # Connect points to the zero difference line
    # for i, true_value in enumerate(ground_t_l):
    #     plt.plot([true_value, true_value], [0, differences_bins[i]], color='blue', linestyle='dotted')
    #     plt.plot([true_value, true_value], [0, differences_digest[i]], color='red', linestyle='dotted')

    # Draw a line at 0 to represent the true value
    plt.axhline(0, color='grey', lw=2, linestyle='--')

    # Label the axes
    plt.xlabel('Batch Number', fontsize=12)
    plt.ylabel('Error Compared to Exact Statistics', fontsize=12)
    # plt.title('Comparison of Estimation Methods')

    # Add a legend
    plt.legend(loc='lower right')

    plt.xticks(batches)

    # Show the plot
    plt.savefig(f'figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()


def plot_exact_KS(ground_t_l, figure_name="figure"):
    batches = range(1, len(ground_t_l) + 1)
    # Plot ground truth
    plt.plot(batches, ground_t_l, label='Exact KS Statistic', linestyle=(0, (1, 10)), marker='o')

    # Add labels, title, and legend
    plt.xlabel('Batch Number')
    plt.ylabel('Exact KS Statistic Value')
    plt.legend()
    plt.xticks(batches)

    # Show the plot
    plt.savefig(f'figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()


def plot_run_times(loop_times: dict, client_times: dict, figure_name="figure"):
    k = len(loop_times.values())

    # Plot ground truth
    plt.plot(loop_times.keys(), loop_times.values(), label='Computing KS from T-Digest')
    plt.plot(loop_times.keys(), client_times.values(), label='Updating the T-Digest object on a client')

    # Add labels, title, and legend
    plt.xlabel('Number of items')
    plt.ylabel('Run time in seconds')
    plt.legend()

    # Show the plot
    plt.savefig(f'../figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()


def plot_errors_lines2(ground_t_l, bins_l, digest_l, figure_name="figure"):
    # Calculate the differences
    differences_bins = [est1 - true for est1, true in zip(bins_l, ground_t_l)]
    differences_digest = [est2 - true for est2, true in zip(digest_l, ground_t_l)]

    # Create a scatter plot
    # plt.figure(figsize=(10, 5))

    batches = range(1, len(ground_t_l) + 1)
    # Plot method 1
    plt.scatter(batches, differences_bins, color='blue', label='Bins', marker='o', s=200)
    # Plot method 2
    plt.scatter(batches, differences_digest, color='red', label='T-Digest', marker='x', s=200)
    #
    # # Connect points to the zero difference line
    # for i, true_value in enumerate(ground_t_l):
    #     plt.plot([true_value, true_value], [0, differences_bins[i]], color='blue', linestyle='dotted')
    #     plt.plot([true_value, true_value], [0, differences_digest[i]], color='red', linestyle='dotted')

    # Draw a line at 0 to represent the true value
    plt.axhline(0, color='grey', lw=2, linestyle='--')

    # Label the axes
    plt.xlabel('Batch Number', fontsize=16)
    plt.ylabel('Error Compared to Exact KS', fontsize=16)
    # plt.title('Comparison of Estimation Methods')

    # Add a legend
    plt.legend(loc='lower right', fontsize=16)

    plt.xticks(batches)

    # Show the plot
    plt.savefig(f'figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()


def plot_exact_KS2(ground_t_l, figure_name="figure"):
    batches = range(1, len(ground_t_l) + 1)
    # Plot ground truth
    plt.plot(batches, ground_t_l, label='Exact KS Statistic', linestyle=(0, (1, 10)), linewidth=2, marker='o',
             markersize=15)

    # Add labels, title, and legend
    plt.xlabel('Batch Number', fontsize=16)
    plt.ylabel('Exact KS Statistic Value', fontsize=16)
    plt.legend(fontsize=16)
    plt.xticks(batches)

    # Show the plot
    plt.savefig(f'figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()
