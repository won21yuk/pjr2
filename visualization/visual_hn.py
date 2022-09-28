import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "seoul_bike.settings")
import django
django.setup()
from seoul_bike.models import *
import pandas as pd


df2 = pd.DataFrame(list(DongCode.objects.all().values()))

print(df2)