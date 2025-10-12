import random

def generate_data(num_samples = 1000):
    data = []
    for _ in range(num_samples):
        sample = {
            'id': random.randint(1, 10000),
            'start_time': random.uniform(0, 1000),
            'end_time': random.uniform(1000, 2000),
        }
        data.append(sample)
    return data


if __name__ == "__main__":
    samples = generate_data(10)
    for sample in samples:
        print(sample)