from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_one():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    SELECT s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    :return:
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    # order_by(Grade.grade.desc())
    return result


def select_two():
    """
    --2. Знайти студента із найвищим середнім балом з певного предмета.

    SELECT d.name, s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.id = 5
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 1;
    :return:
    """
    result = session.query(
        Discipline.name,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline) \
        .filter(Discipline.id == 5) \
        .group_by(Student.id, Discipline.name).order_by(desc('avg_grade')).limit(1).first()
    return result


def select_three():
    """
   --3. Знайти середній бал у групах з певного предмета.

    SELECT gr.name, d.name, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    LEFT JOIN [groups] gr ON gr.id = s.group_id
    WHERE d.id = 1
    GROUP BY gr.id
    ORDER BY avg_grade DESC;
    """
    result = session.query(
        Group.name,
        Discipline.name,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline).join(Group) \
        .filter(Discipline.id == 1) \
        .group_by(Group.name, Discipline.name).order_by(desc('avg_grade')).all()
    return result


def select_four():
    """
    --4. Знайти середній бал на потоці (по всій таблиці оцінок).

    SELECT ROUND(AVG(grade), 2) as avg_grade
    FROM grades g
    :return:
    """
    result = session.query(
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).all()

    return result


def select_five():
    """
    -- 5. Знайти які курси читає певний викладач.

    SELECT name
    FROM disciplines d
    WHERE teacher_id = 2
    :return:
    """
    result = session.query(
        Teacher.fullname,
        Discipline.name) \
        .select_from(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == 3).all()

    return result


def select_six():
    """
    -- 6. Знайти список студентів у певній групі.

    SELECT fullname
    FROM students s
    WHERE group_id = 3
    :return:
    """
    result = session.query(
        Student.fullname,
        Group.name) \
        .select_from(Student) \
        .join(Group) \
        .filter(Group.id == 2).all()
    return result


def select_seven():
    """
    -- 7. Знайти оцінки студентів у окремій групі з певного предмета.

    SELECT *
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    where s.group_id = 1 AND g.discipline_id = 1
    :return:
    """
    result = session.query(
        Student.fullname,
        Group.name,
        Grade.grade,
        Discipline.name) \
        .select_from(Grade).join(Student).join(Group).join(Discipline) \
        .filter(Group.id == 1, Discipline.id == 2).all()
    return result


def select_eight():
    """
    - 8. Знайти середній бал, який ставить певний викладач зі своїх предметів.

    SELECT d.name, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.teacher_id = 2
    GROUP BY g.discipline_id
    :return:
    """
    result = session.query(
        Teacher.fullname,
        Discipline.name,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Discipline).join(Teacher) \
        .filter(Teacher.id == 2) \
        .group_by(Discipline.name, Teacher.fullname).all()
    return result


def select_nine():
    """
    -- 9. Знайти список курсів, які відвідує студент.

    SELECT d.name
    FROM grades g
    JOIN disciplines d ON d.id = g.discipline_id
    WHERE  g.student_id = 2
    GROUP BY g.discipline_id
    :return:
    """
    result = session.query(
        Student.fullname,
        Discipline.name
                        ) \
        .select_from(Grade).join(Discipline).join(Student) \
        .filter(Student.id == 2) \
        .group_by(Discipline.name, Student.fullname).all()
    return result


def select_ten():
    """
    -- 10. Список курсів, які певному студенту читає певний викладач.

    SELECT d.name
    FROM grades g
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE  d.teacher_id = 2 AND g.student_id = 2
    GROUP BY g.discipline_id
    :return:
    """
    result = session.query(
        Discipline.name,
        Student.fullname,
        Teacher.fullname
    ) \
        .select_from(Grade).join(Discipline).join(Student).join(Teacher) \
        .filter(Teacher.id == 3, Student.id == 2) \
        .group_by(Discipline.name, Student.fullname, Teacher.fullname).all()
    return result


if __name__ == '__main__':
    # print(select_one())
    # print(select_two())
    # print(select_three())
    # print(select_four())
    # print(select_five())
    # print(select_six())
    # print(select_seven())
    # print(select_eight())
    # print(select_nine())
    print(select_ten())
