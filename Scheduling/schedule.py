from utils import get_random_number
from copy import deepcopy

from domain import Class

class Schedule(object):
  def __init__(self, data):
    self.data = data
    self._classes = []
    self.class_number = 0
    self._fitness = -1
    self.number_of_conflicts = 0
  
  def __str__(self):
    return "\n".join([str(x) for x in self._classes])
    
  # Methode qui retourne le tableau de cours
  @property
  def classes(self):
    return self._classes

  # Methode qui remplit un emploi du temps avec des cours (ces cours ont un meeting_time, un numero de salle et un prof aleatoire)
  def initialize(self):
    def _create_class(self, course, dept):
      _class = Class(id=self.class_number, department=dept, course=course)
      self.class_number += 1

      _class.meeting_time = deepcopy(self.data.meeting_times[int(len(self.data.meeting_times) * get_random_number())])
      _class.room = deepcopy(self.data.rooms[int(len(self.data.rooms) * get_random_number())])
      _class.instructor = deepcopy(self.data.instructors[int(len(self.data.instructors) * get_random_number())])

      self._classes.append(_class)

    for dept in self.data.depts:
      for course in dept.courses:
        _create_class(self, course, dept)

    return self

  # Methode d'évaluation (fitness correspond a l'inverse du nombre de conflits)
  def calculate_fitness(self):
    number_of_conflicts = 0
    for idx, _class in enumerate(self._classes):
      if _class.room.seating_capacity < _class.course.max_number_of_students:
        number_of_conflicts += 1
      
      for _tmp_class in self._classes[idx:]:
        if _class.meeting_time == _tmp_class.meeting_time and _class.id != _tmp_class.id:

          if _class.room == _tmp_class.room:
            number_of_conflicts += 1
          
          if _class.instructor == _tmp_class.instructor:
            number_of_conflicts += 1

      test = False
      for instructor in _class.course.instructors:
        if instructor == _class.instructor:
          test = True
      
      if not test:
        number_of_conflicts += 1

    self.number_of_conflicts = number_of_conflicts
    
    return (1/(1.0*(self.number_of_conflicts + 1)))