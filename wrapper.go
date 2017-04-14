/**
MIT License

Copyright (c) 2017 levin.lin(林立明)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

     ██████╗ ██████╗  ██████╗
    ██╔═══██╗██╔══██╗██╔═══██╗
    ██║   ██║██████╔╝██║   ██║  ╔══════╗  █                       █
    ██║   ██║██╔═══╝ ██║▄▄ ██║ ╔╝ ▮███ ╚╗ █   ■■  █  █  ■   ■■    █   ■   ■■
    ╚██████╔╝██║     ╚██████╔╝ ║ ▮█     ║ █  █  █ █ █  ■█  █  █   █  ■█  █  █
     ╚═════╝ ╚═╝      ╚══▀▀═╝  ║ ▮█     ║ █  █■■  █ █   █  █  █   █   █  █  █
                               ╚╗ ▮███ ╔╝ ██  ■■   █    ██ █  █ ■ ██  ██ █  █
                                ╚══════╝
*/

package main

import (
	"fmt"
)

// wrapper of fmt.Println
func Println(a ...interface{}) (n int, err error) {
	if "yes" == *debugModel {
		return fmt.Println(a...)
	}
	return 0, nil
}

// wrapper of fmt.Printf
func Printf(format string, a ...interface{}) (n int, err error) {
	if "yes" == *debugModel {
		return fmt.Printf(format, a...)
	}
	return 0, nil
}

// wrapper of fmt.Print
func Print(a ...interface{}) (n int, err error) {
	if "yes" == *debugModel {
		return fmt.Print(a...)
	}
	return 0, nil
}

// wrapper of fmt.Sprintf
func Sprintf(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}
