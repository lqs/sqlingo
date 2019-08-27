package sqlingo

import "unicode"

func CamelName(s string) (result string) {
	nextCharShouldBeUpperCase := true
	for _, ch := range s {
		if ch == '_' {
			nextCharShouldBeUpperCase = true
		} else {
			if nextCharShouldBeUpperCase {
				result += string(unicode.ToUpper(ch))
				nextCharShouldBeUpperCase = false
			} else {
				result += string(ch)
			}
		}
	}
	return
}
