def welcome():
    """Приветствие и базовый цикл команд."""
    print("Первая попытка запустить проект!")
    print("\n***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")

    while True:
        command = input("Введите команду: ").strip().lower()
        if command == "exit":
            print("Выход из программы. До встречи!")
            break
        elif command == "help":
            print("<command> exit - выйти из программы")
            print("<command> help - справочная информация")
        else:
            print("Неизвестная команда. Введите 'help'.")
