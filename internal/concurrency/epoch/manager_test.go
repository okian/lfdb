// Licensed under the MIT License. See LICENSE file in the project root for details.

package epoch

import (
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestManagerBasicOperations(t *testing.T) {
	Convey("Given a new epoch manager", t, func() {
		m := NewManager()

		Convey("Initially", func() {
			So(m.MinActive(), ShouldEqual, 0)
			So(m.ActiveCount(), ShouldEqual, 0)
		})

		Convey("When registering epoch 10", func() {
			m.Register(10)

			Convey("Then MinActive should be 10", func() {
				So(m.MinActive(), ShouldEqual, 10)
			})

			Convey("And ActiveCount should be 1", func() {
				So(m.ActiveCount(), ShouldEqual, 1)
			})

			Convey("When registering epoch 5", func() {
				m.Register(5)

				Convey("Then MinActive should be 5", func() {
					So(m.MinActive(), ShouldEqual, 5)
				})

				Convey("And ActiveCount should be 2", func() {
					So(m.ActiveCount(), ShouldEqual, 2)
				})

				Convey("When unregistering epoch 10", func() {
					m.Unregister(10)

					Convey("Then MinActive should be 5", func() {
						So(m.MinActive(), ShouldEqual, 5)
					})

					Convey("And ActiveCount should be 1", func() {
						So(m.ActiveCount(), ShouldEqual, 1)
					})

					Convey("When unregistering epoch 5", func() {
						m.Unregister(5)

						Convey("Then MinActive should be 0", func() {
							So(m.MinActive(), ShouldEqual, 0)
						})

						Convey("And ActiveCount should be 0", func() {
							So(m.ActiveCount(), ShouldEqual, 0)
						})
					})
				})
			})
		})
	})
}

func TestManagerDuplicateRegistrations(t *testing.T) {
	Convey("Given a new epoch manager", t, func() {
		m := NewManager()

		Convey("When registering epoch 10 multiple times", func() {
			m.Register(10)
			m.Register(10)
			m.Register(10)

			Convey("Then ActiveCount should be 1", func() {
				So(m.ActiveCount(), ShouldEqual, 1)
			})

			Convey("When unregistering once", func() {
				m.Unregister(10)

				Convey("Then ActiveCount should still be 1", func() {
					So(m.ActiveCount(), ShouldEqual, 1)
				})

				Convey("When unregistering twice more", func() {
					m.Unregister(10)
					m.Unregister(10)

					Convey("Then ActiveCount should be 0", func() {
						So(m.ActiveCount(), ShouldEqual, 0)
					})
				})
			})
		})
	})
}

func TestManagerConcurrentAccess(t *testing.T) {
	Convey("Given a new epoch manager", t, func() {
		m := NewManager()

		Convey("When performing concurrent registrations and unregistrations", func() {
			var wg sync.WaitGroup
			const numGoroutines = 10
			const numOps = 1000

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					for j := 0; j < numOps; j++ {
						ts := uint64(goroutineID*numOps + j)
						m.Register(ts)
						m.Unregister(ts)
					}
				}(i)
			}

			wg.Wait()

			Convey("Then all epochs should be unregistered", func() {
				So(m.ActiveCount(), ShouldEqual, 0)
				So(m.MinActive(), ShouldEqual, 0)
			})
		})
	})
}

func TestManagerUnregisterNonExistent(t *testing.T) {
	Convey("Given a new epoch manager", t, func() {
		m := NewManager()

		Convey("When unregistering a non-existent epoch", func() {
			m.Unregister(10)

			Convey("Then ActiveCount should be 0", func() {
				So(m.ActiveCount(), ShouldEqual, 0)
			})

			Convey("And MinActive should be 0", func() {
				So(m.MinActive(), ShouldEqual, 0)
			})
		})
	})
}
