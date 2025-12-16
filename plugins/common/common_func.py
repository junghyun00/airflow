def get_sftp():
    print('sftp 작업을 시작합니다')

def regist(name, sex, *args):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타 옵션들 : {args}')

def regist2(name, sex, *a, **k):
    print(f'이름 : {name}')
    print(f'성별 : {sex}')
    print(f'기타 옵션들 : {a}')
    # email = k['email'] or None   # 해당 키 값이 없는 경우 에러가 남
    # phone = k['phone'] or None
    email = k.get('email') or None # get 함수를 사용하면 키 값이 없는 경우 에러 안 나고 그냥 None 값을 반환함 더 안전한 방법
    phone = k.get('phone') or None
    if email:
        print(email)
    if phone:
        print(phone) 