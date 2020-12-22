from schedule import Schedule
from functools import cmp_to_key

class Population(object):
  def __init__(self, size, data, schedules=None):
    if schedules is None:
      self.schedules = [Schedule(data).initialize() for _ in range(size)]
    else:
      self.schedules = schedules
      if len(schedules)<size:
        for i in range(len(schedules),size):
          self.schedules.append(Schedule(data).initialize())
          self.schedules[i]._fitness = self.schedules[i].calculate_fitness()
        self.sort_by_fitness()
          
  def __str__(self):
    return "".join([str(x) for x in self.schedules])

  # Methode de tri de la population par ordre decroissant de la fitness
  def sort_by_fitness(self):
    self.schedules = sorted(self.schedules, key= lambda schedule: schedule._fitness, reverse=True)
    return self
