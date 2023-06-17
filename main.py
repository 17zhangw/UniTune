import pdb
from MultiTune.advisor.alternative_adviser import TopAdvisor
from MultiTune.utils.parser import parse_args
from MultiTune.database.postgresdb import PostgresDB

import argparse



if __name__ == '__main__':
    if __name__ == '__main__':
        prog = argparse.ArgumentParser(prog="UniTune")
        prog.add_argument("--config-ini", required=True)
        a = prog.parse_args()

        args_db, args_tune = parse_args(a.config_ini)
        db = PostgresDB(args_tune['task_id'], **args_db)
        top_adviser = TopAdvisor(db, args_tune)
        print("Run {}\n".format(args_tune['arm_method']))
        top_adviser.run(args_tune['arm_method'])
