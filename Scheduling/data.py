from domain import (
  Class,
  Course,
  Department,
  Instructor,
  MeetingTime,
  Room
)

class Data(object):
  def __init__(self):
    self.rooms = None
    self.instructors = None
    self.courses = None
    self.depts = None
    self.meeting_times = None
    self.number_of_classes = None

    self.initialize()
  
  def initialize(self):
    # Creation des salles
    room1 = Room(number="EAU", seating_capacity=25)
    room2 = Room(number="BORDEAUX", seating_capacity=45)
    room3 = Room(number="ECHIQUIER", seating_capacity=35)
    self.rooms = [room1, room2, room3]

    # Creation des horaires
    meeting_time1 = MeetingTime(id="MT1", time="MWF 08:00 - 10:00")
    meeting_time2 = MeetingTime(id="MT2", time="MWF 10:00 - 12:00")
    meeting_time3 = MeetingTime(id="MT3", time="TTH 08:00 - 10:00")
    meeting_time4 = MeetingTime(id="MT4", time="TTH 10:00 - 12:00")

    self.meeting_times = [
      meeting_time1, 
      meeting_time2, 
      meeting_time3, 
      meeting_time4
    ]

    # Creation des professeurs
    instructor1 = Instructor(id="I1", name="Algo teacher")
    instructor2 = Instructor(id="I2", name="AI teacher")
    instructor3 = Instructor(id="I3", name="Network teacher")
    instructor4 = Instructor(id="I4", name="Security teacher")
    instructor5 = Instructor(id="I5", name="English teacher")

    self.instructors = [instructor1, instructor2, instructor3, instructor4, instructor5]

    # Creation des cours
    course1 = Course(number="C1", name="ALGO", max_number_of_students=25, instructors=[instructor1, instructor2])
    course2 = Course(number="C2", name="AI", max_number_of_students=35, instructors=[instructor1, instructor2, instructor3])
    course3 = Course(number="C3", name="SECURITY", max_number_of_students=25, instructors=[instructor2, instructor4])
    course4 = Course(number="C4", name="NETWORK", max_number_of_students=30, instructors=[instructor3, instructor4])
    course5 = Course(number="C5", name="PYTHON", max_number_of_students=35, instructors=[instructor1])
    course6 = Course(number="C6", name="DATA_MINING", max_number_of_students=45, instructors=[instructor1, instructor2])
    course7 = Course(number="C7", name="GRAPH", max_number_of_students=45, instructors=[instructor3, instructor4])
    course8 = Course(number="C8", name="JAVA", max_number_of_students=30, instructors=[instructor1, instructor3])
    course9 = Course(number="C9", name="C++", max_number_of_students=25, instructors=[instructor2, instructor3])
    course10 = Course(number="C10", name="LOGIC", max_number_of_students=30, instructors=[instructor1])
    course11 = Course(number="C11", name="ENGLISH", max_number_of_students=25, instructors=[instructor5])

    self.courses = [course1, course2, course3, course4, course5, course6, course7, course8, course9, course10, course11]

    # Create departments
    department1 = Department(name="3 INFO", courses=[course1, course3, course8])
    department2 = Department(name="4 INFO", courses=[course2, course4, course5, course11])
    department3 = Department(name="5 INFO", courses=[course6, course7, course9])

    self.depts = [department1, department2, department3]

    # Definit le nombre de cours
    self.number_of_classes = sum([len(x.courses) for x in self.depts])
