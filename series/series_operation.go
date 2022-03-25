package series

import "fmt"

type arithmeticOperator string

const (
	add       arithmeticOperator = "+"
	substract arithmeticOperator = "-"
	multiply  arithmeticOperator = "*"
	divide    arithmeticOperator = "/"
	modulo    arithmeticOperator = "%"
)

var operationPerColumnType = map[Type]func(left, right Element, operator arithmeticOperator) (interface{}, error){
	String: stringColumnOperation,
	Int:    intColumnOperation,
	Float:  floatColumnOperation,
}

func stringColumnOperation(left, right Element, operator arithmeticOperator) (interface{}, error) {
	switch operator {
	case add:
		return left.String() + right.String(), nil
	default:
		return "", fmt.Errorf("operation %s is not supported in string type", operator)
	}
}

func intColumnOperation(left, right Element, operator arithmeticOperator) (interface{}, error) {
	leftIntVal, err := left.Int()
	if err != nil {
		return nil, err
	}
	rightIntVal, err := right.Int()
	if err != nil {
		return nil, err
	}
	switch operator {
	case add:
		return leftIntVal + rightIntVal, nil
	case substract:
		return leftIntVal - rightIntVal, nil
	case multiply:
		return leftIntVal * rightIntVal, nil
	case divide:
		if rightIntVal == 0 {
			return nil, nil
		}
		return leftIntVal / rightIntVal, nil
	case modulo:
		if rightIntVal == 0 {
			return nil, nil
		}
		return leftIntVal % rightIntVal, nil
	default:
		return "", fmt.Errorf("operation %s is not supported in int type", operator)
	}
}

func floatColumnOperation(left, right Element, operator arithmeticOperator) (interface{}, error) {
	leftFloatVal := left.Float()
	rightFloatVal := right.Float()

	switch operator {
	case add:
		return leftFloatVal + rightFloatVal, nil
	case substract:
		return leftFloatVal - rightFloatVal, nil
	case multiply:
		return leftFloatVal * rightFloatVal, nil
	case divide:
		if rightFloatVal == 0 {
			return nil, nil
		}
		return leftFloatVal / rightFloatVal, nil
	default:
		return "", fmt.Errorf("operation %s is not supported in float type", operator)
	}
}

func (s Series) Add(right Series, indexes Indexes) Series {
	return s.doArithmeticOperation(right, indexes, add)
}

func (s Series) Substract(right Series, indexes Indexes) Series {
	return s.doArithmeticOperation(right, indexes, substract)
}

func (s Series) Multiply(right Series, indexes Indexes) Series {
	return s.doArithmeticOperation(right, indexes, multiply)
}

func (s Series) Divide(right Series, indexes Indexes) Series {
	return s.doArithmeticOperation(right, indexes, divide)
}

func (s Series) Modulo(right Series, indexes Indexes) Series {
	return s.doArithmeticOperation(right, indexes, modulo)
}

func (s Series) doArithmeticOperation(right Series, indexes Indexes, operator arithmeticOperator) Series {
	left := s.Flatten()
	right = right.Flatten()

	isBroadcastLeft := left.Len() == 1
	isBroadcastRight := right.Len() == 1
	if left.Len() != right.Len() && !isBroadcastLeft && !isBroadcastRight {
		return Series{Err: fmt.Errorf("can't do operation due to mismatch length")}
	}
	seriesLen := left.Len()
	if isBroadcastLeft {
		seriesLen = right.Len()
	}

	affectedIndexes, err := getIndexes(seriesLen, indexes)
	if err != nil {
		return Series{Err: err}
	}
	resultType := getArithmeticResultType(left, right)
	columnOperationFn, ok := operationPerColumnType[resultType]

	if !ok {
		return Series{Err: fmt.Errorf("%s is not supported for %s operation", resultType, operator)}
	}

	res := make([]interface{}, seriesLen)
	for _, idx := range affectedIndexes {
		leftIdx := 0
		if !isBroadcastLeft {
			leftIdx = idx
		}
		rightIdx := 0
		if !isBroadcastRight {
			rightIdx = idx
		}

		leftElement := left.elements.Elem(leftIdx)
		rightElement := right.elements.Elem(rightIdx)
		if leftElement.IsNA() || rightElement.IsNA() {
			continue
		}

		opResult, err := columnOperationFn(leftElement, rightElement, operator)
		if err != nil {
			return Series{Err: err}
		}
		res[idx] = opResult
	}
	return New(res, resultType, "")
}

func getIndexes(seriesLen int, indexes Indexes) ([]int, error) {
	if indexes == nil {
		arr := make([]int, seriesLen)
		for i := 0; i < seriesLen; i++ {
			arr[i] = i
		}
		return arr, nil
	}
	return parseIndexes(seriesLen, indexes)
}

func getArithmeticResultType(left, right Series) Type {
	leftType := left.Type()
	rightType := right.Type()
	if leftType == String || rightType == String {
		return String
	}
	if leftType == Bool || rightType == Bool {
		return Bool
	}
	if leftType == Float || rightType == Float {
		return Float
	}
	return leftType
}
