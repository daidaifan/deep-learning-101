class ScalarAddLayer:
    def __init__(self):
        self.x = None
        self.y = None

    def forward(self, x, y):
        self.x = x
        self.y = y
        return self.x + self.y

    def backward(self, dout):
        dx = dout * 1.
        dy = dout * 1.
        return dx, dy


class ScalarMulLayer:
    def __init__(self):
        self.x = None
        self.y = None

    def forward(self, x, y):
        self.x = x
        self.y = y
        return self.x * self.y

    def backward(self, dout):
        dx = dout * self.y
        dy = dout * self.x
        return dx, dy


def main():
    add_layer = ScalarAddLayer()
    result = add_layer.forward(50, 100)
    print(result)
    result = add_layer.backward(200)
    print(result)

    mul_layer = ScalarMulLayer()
    result = mul_layer.forward(30, 60)
    print(result)
    result = mul_layer.backward(90)
    print(result)


if __name__ == '__main__':
    main()
