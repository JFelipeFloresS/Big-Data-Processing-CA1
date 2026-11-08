import subprocess

def main():
    exercises = [1,2,3,4,5,6,7]
    types = ['core', 'dataframes']
    sizes = ['micro', 'complete']

    def script_name(exercise, type):
        return f'A01_ex{exercise}_spark_{type}/A01_ex{exercise}_spark_{type}.py'

    def dataset_location(exercise, size):
        location = '../FileStore/tables/my_datasets/'
        if size == 'complete':
            return location + 'my_dataset_complete/'

        return location + f'A01_micro_dataset_{exercise}/'

    def solution_location(exercise, size):
        location = f'../my_results/Student_Solutions/A01_ex{exercise}_spark_dataframes/'
        if size == 'complete':
            return location + f'A01_ex{exercise}_spark_core_entire_dataset.txt'

        return location + f'A01_ex{exercise}_spark_dataframes_small_dataset.txt'

    def call_args(exercise):
        if exercise == 1:
            return ['279']
        elif exercise == 2:
            return ['40']
        elif exercise == 3:
            return []
        elif exercise == 4:
            return ["40,72", "08,09", 60]
        elif exercise == 5:
            return [40, "2013-01-07", 600]
        elif exercise == 6:
            return ["2013-01-09 06:00:00", 1800]
        elif exercise == 7:
            return [5.0, 28.0, "2013-01-07"]
        else:
            raise Exception(f'Unknown exercise: {exercise}')

    # go to the directory of the current file
    def run_exercise(exercise, type, size):
        sol_loc = solution_location(exercise, size)

        with open(sol_loc, 'w') as f:
            # build the command python3 {script_name} "{dataset_location}" {call_arguments} > "{solution_location}"
            subprocess.run(["python3", script_name(exercise, type), dataset_location(exercise, size)] + [str(arg) for arg in  call_args(exercise)], stdout=f)

    for size in sizes:
        for type in types:
            for exercise in exercises:
                print("Running exercise", exercise, type, size)
                run_exercise(exercise, type, size)




if __name__ == "__main__":
    main()