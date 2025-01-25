const test = require('tape')

test('test idk', async (t) => {
  t.plan(2)
  t.ok({}, 'obj ok')
  t.equal(1, 1, '1=1')
  t.teardown(() => console.log('close'))
})
