import requests
import os
import pandas as pd
import numpy as np
import re
import psycopg2
import traceback

from congress.etl.vars import TABLE_CREATE_VOTES_SUMMARY, TABLE_CREATE_VOTES_INDIVIDUAL